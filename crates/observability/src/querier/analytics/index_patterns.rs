use krabka_units::convert::ByteSizeExt;

use crate::{
    BTreeMap, BTreeSet, HeaderMap, HttpQueryError, QuerierState, QueryError, SeriesParams,
    StreamPlan, TimeRange, Value, active_log_delete_filters, authorized_tenant,
    collect_detected_fields, is_deleted_log_entry, json, loki_success_value,
    parse_detected_fields_params, parse_detected_labels_params, parse_patterns_params, parse_query,
    parse_query_params, plan_stream_query, planned_block_bytes, read_log_block,
    read_log_block_from_object_store, sample_time_bucket, series_data,
    validate_loki_volume_query_range_limit, validate_query_bytes_limit,
    validate_query_length_limit, validate_query_range_limit, validate_query_series_limit,
};

pub(crate) async fn execute_index_stats_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_query_params(raw_query)?;
    let tenant = authorized_tenant(state, headers).await?;
    let start = params
        .start
        .ok_or(HttpQueryError::MissingQueryParameter("start"))?;
    let end = params
        .end
        .ok_or(HttpQueryError::MissingQueryParameter("end"))?;
    let time_range = TimeRange::new(start, end).map_err(HttpQueryError::from)?;
    validate_loki_volume_query_range_limit(time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
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

pub(crate) async fn count_index_stats_entries(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<u64, HttpQueryError> {
    let mut entries = 0_u64;
    for block in &plan.blocks {
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            read_log_block(&state.root, &block.key)?
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

pub(crate) async fn execute_patterns_query(
    state: &QuerierState,
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

    let tenant = authorized_tenant(state, headers).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let query = parse_query(&params.query).map_err(|source| HttpQueryError::LokiParse {
        query: params.query.clone(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        query,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;

    let mut patterns = BTreeMap::<String, BTreeMap<i64, u64>>::new();
    for block in &plan.blocks {
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            read_log_block(&state.root, &block.key)?
        };
        for row in rows {
            if !plan.fingerprints.contains(&row.series_fingerprint)
                || row.timestamp_ns < plan.time_range.start_ns
                || row.timestamp_ns >= plan.time_range.end_ns
            {
                continue;
            }
            let labels = state
                .label_index
                .labels_for(tenant, row.series_fingerprint)
                .ok_or(QueryError::MissingSeriesLabels {
                    tenant: tenant.to_string(),
                    fingerprint: row.series_fingerprint,
                })?;
            if is_deleted_log_entry(
                &delete_filters,
                labels,
                &row.line,
                &row.structured_metadata,
                row.timestamp_ns,
            ) {
                continue;
            }
            if !plan
                .query
                .matches_with_fields(labels, &row.line, &row.structured_metadata)
            {
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
        .map(|(pattern, samples)| {
            json!({
                "pattern": pattern,
                "samples": samples
                    .into_iter()
                    .map(|(timestamp_ns, count)| json!([timestamp_ns / 1_000_000_000, count]))
                    .collect::<Vec<_>>(),
            })
        })
        .collect::<Vec<_>>();

    Ok(loki_success_value(data))
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
    let templatized = Value::Object(
        map.iter()
            .map(|(key, value)| (key.clone(), json_value_pattern(value)))
            .collect(),
    );
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
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), json_value_pattern(value)))
                .collect(),
        ),
    }
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

pub(crate) fn log_pattern_token(token: &str) -> String {
    let Some((key, value)) = token.split_once('=') else {
        return if pattern_value_is_variable(token) {
            "<_>".to_string()
        } else {
            token.to_string()
        };
    };
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

pub(crate) async fn execute_detected_fields_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_fields_params(raw_query)?;
    let limit = params.limit;
    let fields = collect_detected_fields(state, headers, &params).await?;
    let fields = fields
        .into_iter()
        .take(limit)
        .map(|(label, stats)| {
            let ty = stats.ty.as_loki_str();
            let cardinality = stats.values.len();
            let parsers = stats.parsers_json();
            json!({
                "label": label,
                "type": ty,
                "cardinality": cardinality,
                "parsers": parsers,
            })
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
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_labels_params(raw_query)?;
    let time_range = TimeRange::new(params.start, params.end)?;
    validate_loki_volume_query_range_limit(time_range)?;
    if let Some(query) = &params.query {
        validate_query_length_limit(state, query)?;
    }
    let series_params = SeriesParams {
        matchers: params.query.into_iter().collect(),
        start: Some(params.start),
        end: Some(params.end),
        since: None,
    };
    let label_sets = series_data(state, headers, &series_params).await?;
    let mut values_by_label = BTreeMap::<String, BTreeSet<String>>::new();
    for labels in label_sets {
        for (name, value) in labels {
            values_by_label.entry(name).or_default().insert(value);
        }
    }
    if values_by_label.is_empty() {
        return Ok(json!({}));
    }

    let detected_labels = values_by_label
        .into_iter()
        .take(params.limit)
        .map(|(label, values)| {
            json!({
                "label": label,
                "cardinality": values.len(),
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "detectedLabels": detected_labels,
    }))
}

pub(crate) async fn execute_detected_field_values_query(
    state: &QuerierState,
    headers: &HeaderMap,
    name: &str,
    raw_query: Option<&str>,
) -> Result<Value, HttpQueryError> {
    let params = parse_detected_fields_params(raw_query)?;
    let limit = params.limit;
    let fields = collect_detected_fields(state, headers, &params).await?;
    let values = fields
        .get(name)
        .map(|stats| stats.values.iter().take(limit).cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    if values.is_empty() {
        return Ok(json!({}));
    }

    Ok(json!({
        "values": values,
        "limit": limit,
    }))
}
