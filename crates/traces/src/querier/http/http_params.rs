use krabka_traceql::next_duration_part;

use super::*;

pub(crate) fn accepts_media_type(headers: &HeaderMap, media_types: &[&str]) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| {
            accept.split(',').any(|part| {
                let media_type = part.split(';').next().unwrap_or_default().trim();
                media_types
                    .iter()
                    .any(|expected| media_type.eq_ignore_ascii_case(expected))
            })
        })
}

pub(crate) fn base64<const N: usize>(bytes: [u8; N]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

pub(crate) fn decode_trace_id(trace_id: &str) -> Result<[u8; 16], hex::FromHexError> {
    let mut out = [0; 16];
    hex::decode_to_slice(trace_id, &mut out)?;
    Ok(out)
}

/// Default query-range step when the request supplies none.
///
/// This aims for about 100 buckets over the range, rounded up to a whole
/// second, with a 1s floor. It mirrors Tempo's `DefaultQueryRangeStep` closely
/// enough for a usable series.
pub(crate) fn default_query_range_step_ns(start_ns: UnixNano, end_ns: UnixNano) -> i64 {
    const SECOND_NS: i64 = 1_000_000_000;
    let delta = end_ns.0.saturating_sub(start_ns.0).max(0);
    let raw = delta / 100;
    let rounded = raw.saturating_add(SECOND_NS - 1) / SECOND_NS * SECOND_NS;
    rounded.max(SECOND_NS)
}

pub(crate) fn duration_param(uri: &Uri, key: &str) -> Result<Option<Time>, String> {
    query_param(uri, key)
        .map(|value| {
            parse_go_duration_ns(&value)
                .map(|nanos| Time::from_nanos(i64::try_from(nanos).unwrap_or(i64::MAX)))
                .map_err(|err| format!("invalid {key}: {err}"))
        })
        .transpose()
}

pub(crate) fn exact_tag_value_filter(
    query: &str,
    tag: &str,
) -> Result<Option<TypedValue>, TraceqlError> {
    let query = krabka_traceql::parse(query)?;
    if !query.pipeline.is_empty() {
        return Ok(None);
    }
    let SpansetExpr::Selector(expr) = query.root else {
        return Ok(None);
    };
    let FieldExpr::Comparison { lhs, op, rhs } = *expr else {
        return Ok(None);
    };
    if op != ComparisonOp::Eq || !field_matches_tag(&lhs, tag) {
        return Ok(None);
    }
    Ok(typed_traceql_value(&rhs))
}

pub(crate) fn field_matches_tag(field: &Field, tag: &str) -> bool {
    let tag = tag.strip_prefix('.').unwrap_or(tag);
    match &field.scope {
        Scope::Both => tag == field.key.as_str(),
        Scope::Span => tag == format!("span.{}", field.key),
        Scope::Resource => tag == format!("resource.{}", field.key),
        Scope::Parent => tag == format!("parent.{}", field.key),
        Scope::Event => tag == format!("event.{}", field.key),
        Scope::Link => tag == format!("link.{}", field.key),
        Scope::Instrumentation => tag == format!("instrumentation.{}", field.key),
        Scope::Intrinsic(intrinsic) => tag == intrinsic.tag_name(),
    }
}

pub(crate) fn filter_metrics_exemplars(
    mut resp: TraceMetricsResponse,
    selection: ExemplarSelection,
) -> TraceMetricsResponse {
    match selection {
        ExemplarSelection::All => {}
        ExemplarSelection::Limit(max) => {
            for series in &mut resp.series {
                series.exemplars.truncate(max);
            }
        }
        ExemplarSelection::None => {
            for series in &mut resp.series {
                series.exemplars.clear();
            }
        }
    }
    resp
}

pub(crate) fn filter_search_duration(
    mut resp: SearchResponse,
    min_duration: Option<Time>,
    max_duration: Option<Time>,
    limit: usize,
) -> SearchResponse {
    if min_duration.is_none() && max_duration.is_none() {
        return resp;
    }
    resp.traces.retain(|trace| {
        min_duration.is_none_or(|min| trace.duration >= min)
            && max_duration.is_none_or(|max| trace.duration <= max)
    });
    resp.inspected_traces = resp.traces.len();
    resp.traces.truncate(limit);
    resp
}

pub(crate) fn filter_tag_values(values: Vec<TypedValue>, expected: &TypedValue) -> Vec<TypedValue> {
    values
        .into_iter()
        .filter(|value| value == expected)
        .collect()
}

pub(crate) fn is_match_all_query(query: &str) -> bool {
    query
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .eq("{}".chars())
}

pub(crate) fn optional_seconds_param(uri: &Uri, key: &'static str) -> Result<Option<i64>, String> {
    query_param(uri, key)
        .map(|value| {
            parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
        })
        .transpose()
}

pub(crate) fn optional_time_bounds(uri: &Uri) -> Result<TimeBounds, String> {
    let start_ns = optional_seconds_param(uri, "start")?.unwrap_or(0);
    let end_ns = optional_seconds_param(uri, "end")?.unwrap_or(i64::MAX);
    if end_ns < start_ns {
        return Err("end must be >= start".to_string());
    }
    Ok(TimeBounds { start_ns, end_ns })
}

pub(crate) fn optional_usize_param(uri: &Uri, key: &'static str) -> Result<Option<usize>, String> {
    query_param(uri, key)
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("invalid query parameter {key}"))
        })
        .transpose()
}

pub(crate) fn parse_duration_component_ns(number: &str, multiplier: u128) -> Result<u128, String> {
    let separator = number.find('.');
    let whole = separator.map_or(number, |index| &number[..index]);
    let fraction = separator.map_or("", |index| &number[index + 1..]);
    if whole.is_empty() && fraction.is_empty() {
        return Err(format!("invalid number {number:?}"));
    }
    if fraction.contains('.') {
        return Err(format!("invalid number {number:?}"));
    }

    let whole = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<u128>()
            .map_err(|_| format!("invalid number {number:?}"))?
    };
    let whole_ns = whole
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?;
    if fraction.is_empty() {
        return Ok(whole_ns);
    }

    let fraction_value = fraction
        .parse::<u128>()
        .map_err(|_| format!("invalid number {number:?}"))?;
    let scale = (0..fraction.len())
        .try_fold(1_u128, |acc, _| acc.checked_mul(10))
        .ok_or_else(|| "duration out of range".to_string())?;
    let fraction_ns = fraction_value
        .checked_mul(multiplier)
        .ok_or_else(|| "duration out of range".to_string())?
        / scale;
    whole_ns
        .checked_add(fraction_ns)
        .ok_or_else(|| "duration out of range".to_string())
}

pub(crate) fn parse_go_duration_ns(value: &str) -> Result<u64, String> {
    if value.is_empty() {
        return Err("empty duration".into());
    }

    let mut total = 0_u128;
    let mut rest = value;
    while !rest.is_empty() {
        let part =
            next_duration_part(rest).ok_or_else(|| format!("expected number in {value:?}"))?;
        let number = part.number;
        if part.unit.is_empty() {
            return Err(format!("expected unit after {number:?}"));
        }
        let unit = part.unit;
        let multiplier = match unit {
            "ns" => 1,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(format!("unsupported unit {unit:?}")),
        };
        total = total
            .checked_add(parse_duration_component_ns(number, multiplier)?)
            .ok_or_else(|| "duration out of range".to_string())?;
        rest = part.rest;
    }

    u64::try_from(total).map_err(|_| "duration out of range".into())
}

pub(crate) fn parse_seconds_to_ns(value: &str) -> Option<i64> {
    let negative = value.starts_with('-');
    let value = value.strip_prefix('-').unwrap_or(value);
    let separator = value.find('.');
    let whole = separator.map_or(value, |index| &value[..index]);
    let fraction = separator.map_or("", |index| &value[index + 1..]);
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > 9
    {
        return None;
    }

    let whole_ns = whole.parse::<i64>().ok()?.checked_mul(1_000_000_000)?;
    let fraction_ns = if fraction.is_empty() {
        0
    } else {
        let padded = format!("{fraction:0<9}");
        padded.parse::<i64>().ok()?
    };
    let ns = whole_ns.checked_add(fraction_ns)?;
    if negative { ns.checked_neg() } else { Some(ns) }
}

pub(crate) fn parse_step_to_ns(value: &str) -> Option<i64> {
    parse_seconds_to_ns(value).or_else(|| i64::try_from(parse_go_duration_ns(value).ok()?).ok())
}

pub(crate) fn q_filter_limit(
    uri: &Uri,
    max_traces: usize,
    autocomplete_limit: usize,
) -> Result<usize, String> {
    Ok(
        optional_usize_param(uri, "limit")?.map_or(usize::MAX, |limit| {
            if limit == 0 {
                max_traces.min(autocomplete_limit)
            } else {
                limit.min(max_traces).min(autocomplete_limit)
            }
        }),
    )
}

pub(crate) fn query_param(uri: &Uri, key: &str) -> Option<String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .find_map(|pair| (pair.0 == key).then(|| pair.1.into_owned()))
}

/// Resolves the tenant a query names and checks that `principal` may read it, or gives the response that rejects the query.
///
/// A malformed tenant is a 400. A tenant outside the principal's grant is a
/// 403. Both answers come before the query reads anything.
pub(crate) fn request_tenant(
    headers: &HeaderMap,
    principal: &Principal,
    policy: &TenantPolicy,
) -> Result<TenantId, Box<Response>> {
    // Tempo with multi-tenancy off ignores this header. Krabka does not: it
    // isolates every read by tenant. So a malformed value is a 400 here, and
    // the query never runs as the fallback tenant.
    let tenant = TenantId::resolve(
        headers.get(TENANT_HEADER).map(HeaderValue::as_bytes),
        policy,
    )
    .map_err(|err| {
        Box::new(status_text_response(
            StatusCode::BAD_REQUEST,
            err.to_string(),
        ))
    })?;
    authorize_tenant(principal, &tenant).map_err(|denied| Box::new(denied.into_response()))?;
    Ok(tenant)
}

pub(crate) fn required_seconds_param(uri: &Uri, key: &str) -> Result<i64, String> {
    let Some(value) = query_param(uri, key) else {
        return Err(format!("missing query parameter {key}"));
    };
    parse_seconds_to_ns(&value).ok_or_else(|| format!("invalid query parameter {key}"))
}

/// The required `start` and `end` of a request, in nanoseconds, or the 400
/// that rejects a missing, malformed, or reversed range.
pub(crate) fn required_time_range(uri: &Uri) -> Result<TimeBounds, Box<Response>> {
    let start_ns = required_seconds_param(uri, "start")
        .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    let end_ns = required_seconds_param(uri, "end")
        .map_err(|err| Box::new(status_text_response(StatusCode::BAD_REQUEST, err)))?;
    if end_ns < start_ns {
        return Err(Box::new(status_text_response(
            StatusCode::BAD_REQUEST,
            "end must be >= start",
        )));
    }
    Ok(TimeBounds { start_ns, end_ns })
}

pub(crate) fn scan_options_param(uri: &Uri) -> Result<ScanOptions, String> {
    let block = query_param(uri, "block");
    let row_group_start = optional_usize_param(uri, "rowGroupStart")?;
    let row_group_end = optional_usize_param(uri, "rowGroupEnd")?;
    if block.is_none() && row_group_start.is_none() && row_group_end.is_none() {
        return Ok(ScanOptions::default());
    }
    let Some(object_key) = block else {
        return Err("missing query parameter block".into());
    };
    let Some(row_group_start) = row_group_start else {
        return Err("missing query parameter rowGroupStart".into());
    };
    let Some(row_group_end) = row_group_end else {
        return Err("missing query parameter rowGroupEnd".into());
    };
    if row_group_end <= row_group_start {
        return Err("rowGroupEnd must be > rowGroupStart".into());
    }
    Ok(ScanOptions {
        job: Some(ScanJob {
            object_key,
            row_group_start,
            row_group_end,
        }),
        ..ScanOptions::default()
    })
}

pub(crate) fn scope_param(uri: &Uri) -> Result<Option<TagScope>, &'static str> {
    query_param(uri, "scope")
        .map(|scope| tag_scope_from_name(&scope).ok_or("invalid scope"))
        .transpose()
}

pub(crate) fn step_param(
    uri: &Uri,
    start_ns: UnixNano,
    end_ns: UnixNano,
) -> Result<i64, &'static str> {
    let Some(step) = query_param(uri, "step") else {
        // Tempo computes a default step when the client omits it; Grafana's
        // Traces Drilldown breakdown queries send no `step`. Match that instead
        // of rejecting the query.
        return Ok(default_query_range_step_ns(start_ns, end_ns));
    };
    let Some(step_ns) = parse_step_to_ns(&step) else {
        return Err("invalid step");
    };
    if step_ns <= 0 {
        return Err("step must be positive");
    }
    Ok(step_ns)
}

pub(crate) fn tag_scope_name(scope: TagScope) -> &'static str {
    crate::frontend::merge::scope_key(scope)
}

pub(crate) fn tempo_tag_alias(tag: &str) -> &str {
    match tag.strip_prefix('.').unwrap_or(tag) {
        "name" => "span:name",
        "status" => "span:status",
        tag => tag,
    }
}

pub(crate) fn wants_json(headers: &HeaderMap) -> bool {
    accepts_media_type(headers, &["application/json"])
}

pub(crate) fn wants_protobuf(headers: &HeaderMap) -> bool {
    accepts_media_type(headers, &["application/protobuf", "application/x-protobuf"])
}
