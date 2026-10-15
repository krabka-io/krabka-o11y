use krabka_units::convert::ByteSizeExt;

use crate::{
    AnalyticsQuery, BTreeMap, BTreeSet, HeaderMap, HttpQueryError, QuerierState, RangeEnd,
    RequestSecurity, SeriesParams, StreamPlan, TenantErrorSurface, TimeRange, Value,
    VolumeRangeLimit, authorized_tenant, collect_detected_fields, json, loki_success_value,
    parse_detected_fields_params, parse_detected_labels_params, parse_patterns_params,
    parse_query_params, plan_analytics_query, planned_block_bytes, read_planned_log_block,
    reads_planned_row, sample_time_bucket, series_data, validate_loki_volume_query_range_limit,
    validate_query_string_bytes_limit,
};

struct DetectedFieldsQuery {
    limit: usize,
    fields: BTreeMap<String, crate::DetectedFieldStats>,
}

async fn detected_fields_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<DetectedFieldsQuery, HttpQueryError> {
    let params = parse_detected_fields_params(raw_query)?;
    let limit = params.limit;
    let fields = collect_detected_fields(state, security, headers, &params).await?;
    Ok(DetectedFieldsQuery { limit, fields })
}

/// A long pure-hex string, such as a trace or span id, a digest, or a dash-less
/// UUID. The length floor keeps short hex-looking words such as `face` and
/// `cafe` out of the templatize path.
pub(crate) fn is_hex_id(value: &str) -> bool {
    value.len() >= 16 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

/// A long opaque identifier, such as a session token, a base62 id, or an api
/// key. It is purely alphanumeric with both letters and digits. The required
/// digit keeps long lowercase or mixed-case words out of the templatize path,
/// and the punctuation exclusion keeps module paths and file locations
/// intact.
pub(crate) fn is_high_entropy_id(value: &str) -> bool {
    value.len() >= 16
        && value.bytes().all(|b| b.is_ascii_alphanumeric())
        && value.bytes().any(|b| b.is_ascii_digit())
        && value.bytes().any(|b| b.is_ascii_alphabetic())
}

/// Canonical `8-4-4-4-12` hex UUID, regardless of leading character.
pub(crate) fn is_uuid(value: &str) -> bool {
    let mut groups = value.split('-');
    let shaped = [8usize, 4, 4, 4, 12].into_iter().all(|len| {
        groups
            .next()
            .is_some_and(|group| group.len() == len && group.bytes().all(|b| b.is_ascii_hexdigit()))
    });
    shaped && groups.next().is_none()
}

pub(crate) async fn count_index_stats_entries(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<u64, HttpQueryError> {
    let mut entries = 0_u64;
    for block in &plan.blocks {
        // A block that is gone contributes no rows to the count. The count
        // is then short by that block, which is what the log line reports.
        let Some(rows) = read_planned_log_block(state, &block.key).await? else {
            continue;
        };
        let matching_entries = rows
            .into_iter()
            .filter(|row| {
                plan.fingerprints.contains(&row.series_fingerprint)
                    && plan.time_range.start_ns <= row.timestamp_ns
                    && row.timestamp_ns <= plan.time_range.end_ns
            })
            .count();
        entries = entries.saturating_add(u64::try_from(matching_entries).unwrap_or(u64::MAX));
    }
    Ok(entries)
}

/// Templatizes a single-object JSON log line. Returns `None` for anything that
/// is not a JSON object, so the caller falls back to whitespace or logfmt
/// mining.
pub(crate) fn json_log_pattern(line: &str) -> Option<String> {
    // `from_str` already rejects non-objects and non-JSON, so there is no
    // cheap pre-check guard here: a leading-`{` fast path would be a pure
    // performance optimization with no behavior of its own to test.
    let Value::Object(map) = serde_json::from_str::<Value>(line.trim()).ok()? else {
        return None;
    };
    let mut templatized_map = serde_json::Map::new();
    for entry in &map {
        templatized_map.insert(entry.0.clone(), json_value_pattern(entry.1));
    }
    let templatized = Value::Object(templatized_map);
    serde_json::to_string(&templatized).ok()
}

/// Replaces variable JSON leaf values with the `<_>` placeholder. It keeps the
/// object and array structure, and it keeps constant, low-entropy, values.
pub(crate) fn json_value_pattern(value: &Value) -> Value {
    match value {
        // Numbers are always high-cardinality dimensions (offsets, durations,
        // counts), so collapse them; booleans and null are constants worth
        // keeping as discriminators.
        Value::Number(_) => Value::String("<_>".to_string()),
        Value::Null | Value::Bool(_) => value.clone(),
        Value::String(text) => Value::String(templatize_text(text)),
        Value::Array(items) => Value::Array(items.iter().map(json_value_pattern).collect()),
        Value::Object(map) => {
            let mut templatized_map = serde_json::Map::new();
            for entry in map {
                templatized_map.insert(entry.0.clone(), json_value_pattern(entry.1));
            }
            Value::Object(templatized_map)
        }
    }
}

pub(crate) fn log_line_pattern(line: &str) -> String {
    // Krabka services (and every JSON-emitting collector) log compact objects
    // like `{"timestamp":"…","severity":"INFO","message":"connection opened"}`.
    // Whitespace tokenization mangles those — the quoted values contain spaces
    // and the `:` separator is invisible to the logfmt `key=value` splitter — so
    // every distinct timestamp became its own pattern. Templatize JSON lines
    // structurally instead, keeping keys and constant values while collapsing
    // variable values (timestamps, ids, numbers) to `<_>`.
    if let Some(pattern) = json_log_pattern(line) {
        return pattern;
    }
    line.split_whitespace()
        .map(log_pattern_token)
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) fn log_pattern_token(token: &str) -> String {
    let Some(separator) = token.find('=') else {
        return if pattern_value_is_variable(token) {
            "<_>".to_string()
        } else {
            token.to_string()
        };
    };
    let key = &token[..separator];
    let value = &token[separator + 1..];
    if key.is_empty() || value.is_empty() {
        return token.to_string();
    }
    if pattern_value_is_variable(value) {
        format!("{key}=<_>")
    } else {
        token.to_string()
    }
}

/// Whether a token looks like variable data, that is data that varies per line,
/// which the pattern should templatize to `<_>` instead of keeping as a
/// constant part.
///
/// The leading-digit and float checks catch timestamps and other
/// numeric-leading values. Identifiers that begin with a letter, such as
/// UUIDs, trace and span hashes, and opaque high-entropy tokens, need the
/// explicit shape checks, so they do not each become their own pattern.
pub(crate) fn pattern_value_is_variable(value: &str) -> bool {
    let value = value.trim_matches('"');
    if value.is_empty() {
        return false;
    }
    value.as_bytes().first().is_some_and(u8::is_ascii_digit)
        || value.parse::<f64>().is_ok()
        || is_uuid(value)
        || is_hex_id(value)
        || is_high_entropy_id(value)
}

/// Templatizes the variable whitespace-delimited tokens inside a free-text
/// value, for example an embedded request id or timestamp in a `message`
/// field. It leaves the constant words intact, so distinct messages stay
/// distinct patterns.
pub(crate) fn templatize_text(text: &str) -> String {
    text.split_whitespace()
        .map(|token| {
            if pattern_value_is_variable(token) {
                "<_>".to_string()
            } else {
                token.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub(crate) async fn execute_detected_field_values_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    name: &str,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let DetectedFieldsQuery { limit, fields } =
        detected_fields_query(state, security, headers, raw_query).await?;
    let values = fields.get(name).map_or_else(Default::default, |stats| {
        stats.values.iter().take(limit).cloned().collect::<Vec<_>>()
    });
    if values.is_empty() {
        return Ok(json!({}));
    }

    Ok(json!({
        "values": values,
        "limit": limit,
    }))
}

pub(crate) async fn execute_detected_fields_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let DetectedFieldsQuery { limit, fields } =
        detected_fields_query(state, security, headers, raw_query).await?;
    let fields = fields
        .into_iter()
        .take(limit)
        .map(|field_entry| {
            let label = field_entry.0;
            let field_stats = field_entry.1;
            let ty = field_stats.ty.as_loki_str();
            let cardinality = field_stats.values.len();
            // Loki names the JSON path a `json`-parsed field was read from, so
            // a client can build a `| json name="path"` stage from it. Only
            // top-level keys are detected here, so the path is the key.
            let json_path = field_stats.parsers.contains("json").then(|| json!([label]));
            let parsers = field_stats.parsers_json();
            let mut field = json!({
                "label": label,
                "type": ty,
                "cardinality": cardinality,
                "parsers": parsers,
            });
            if let Some(json_path) = json_path {
                field["jsonPath"] = json_path;
            }
            field
        })
        .collect::<Vec<_>>();
    if fields.is_empty() {
        return Ok(json!({}));
    }

    Ok(json!({
        "fields": fields,
        "limit": limit,
    }))
}

pub(crate) async fn execute_detected_labels_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_labels_params(raw_query)?;
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Read).await?;
    let state = &state.with_tenant_limits(&tenant);
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(state, time_range)?;
    if let Some(query) = &params.query {
        validate_query_string_bytes_limit(state, query)?;
    }
    let series_params = SeriesParams {
        matchers: params.query.into_iter().collect(),
        start: Some(params.start),
        end: Some(params.end),
        since: None,
    };
    let label_sets = series_data(state, security, headers, &series_params).await?;
    let mut values_by_label = BTreeMap::<String, BTreeSet<String>>::new();
    for labels in label_sets {
        for label in labels {
            values_by_label
                .entry(label.0.clone())
                .or_default()
                .insert(label.1.clone());
        }
    }
    if values_by_label.is_empty() {
        return Ok(json!({}));
    }

    let detected_labels = values_by_label
        .into_iter()
        .take(params.limit)
        .map(|entry| {
            json!({
                "label": entry.0,
                "cardinality": entry.1.len(),
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "detectedLabels": detected_labels,
    }))
}

pub(crate) async fn execute_index_stats_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    let tenant = authorized_tenant(state, security, headers, TenantErrorSurface::Read).await?;
    let start = params
        .start
        .ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end = params
        .end
        .ok_or(HttpQueryError::MissingQueryParameter("end"))?;
    let time_range = TimeRange::new(start, end)?;
    let planned = plan_analytics_query(
        state,
        AnalyticsQuery {
            tenant: &tenant,
            time_range,
            query: &params.query,
            volume_range_limit: VolumeRangeLimit::Check,
        },
    )
    .await?;
    let state = planned.state;
    let plan = planned.plan;
    let entries = count_index_stats_entries(&state, &plan).await?;
    let bytes = planned_block_bytes(&plan).bytes_u64();
    let streams = plan
        .blocks
        .iter()
        .flat_map(|block| block.fingerprints.iter())
        .filter(|fingerprint| plan.fingerprints.contains(fingerprint))
        .copied()
        .collect::<BTreeSet<_>>()
        .len();

    Ok(json!({
        "streams": u64::try_from(streams).unwrap_or(u64::MAX),
        "chunks": u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX),
        "entries": entries,
        "bytes": bytes,
    }))
}

pub(crate) async fn execute_patterns_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_patterns_params(raw_query)?;
    if params.step <= 0 {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "step",
            value: params.step.to_string(),
        });
    }

    let planned = crate::querier::analytics::authorize_and_prepare_analytics_read(
        state,
        security,
        headers,
        TenantErrorSurface::Patterns,
        params.start,
        params.end,
        &params.query,
        VolumeRangeLimit::Skip,
    )
    .await?;
    let read = planned.read(RangeEnd::Exclusive);

    let mut patterns = BTreeMap::<String, BTreeMap<i64, u64>>::new();
    for block in &planned.plan.blocks {
        let Some(rows) = read_planned_log_block(&planned.state, &block.key).await? else {
            continue;
        };
        for row in rows {
            if !reads_planned_row(&read, &row)? {
                continue;
            }
            let bucket = sample_time_bucket(row.timestamp_ns, params.start, params.step);
            *patterns
                .entry(log_line_pattern(&row.line))
                .or_default()
                .entry(bucket)
                .or_default() += 1;
        }
    }

    let data = patterns
        .into_iter()
        .map(|entry| {
            json!({
                "pattern": entry.0,
                "samples": entry.1
                    .into_iter()
                    .map(|(timestamp_ns, count)| json!([timestamp_ns / 1_000_000_000, count]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    Ok(loki_success_value(data))
}
