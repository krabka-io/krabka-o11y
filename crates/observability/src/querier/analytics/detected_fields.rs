use crate::{ActiveLogDeleteFilter, LogRow, TenantId};
use krabka_units::convert::ByteSizeExt;

use crate::{
    BTreeMap, DetectedFieldStats, DetectedFieldType, DetectedFieldsParams, HeaderMap,
    HttpQueryError, Labels, QuerierState, QueryError, RequestSecurity, StreamPlan,
    TenantErrorSurface, TimeRange, Value, VolumeAggregateBy, VolumeKind, VolumeParams,
    WalLogRecord, add_loki_query_stats_for_stream_plan, clamp_query_lookback, current_unix_time_ns,
    is_deleted_log_entry, json, loki_success_value, parse_query, parse_volume_params,
    plan_stream_query, read_planned_log_block, unix_ns_string_to_loki_seconds,
    validate_loki_volume_query_range_limit, validate_query_bytes_limit, validate_query_range_limit,
    validate_query_series_limit, validate_query_string_bytes_limit,
};

pub(crate) fn detect_logfmt_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    for pair in parse_logfmt_pairs(line) {
        let ty = field_type_from_str(&pair.value);
        add_detected_field(fields, &pair.name, pair.value, ty, "logfmt");
    }
}

pub(crate) fn detected_bytes_unit(unit: &str) -> Option<()> {
    match unit {
        "B" | "kB" | "KB" | "MB" | "GB" | "TB" | "KiB" | "MiB" | "GiB" | "TiB" => Some(()),
        _ => None,
    }
}

pub(crate) fn detected_json_value_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
    }
}

pub(crate) fn project_labels(labels: &Labels, target_labels: &[String]) -> Labels {
    let mut projected = Labels::new();
    for name in target_labels {
        if let Some(value) = labels.get(name) {
            projected.insert(name.clone(), value.clone());
        }
    }
    projected
}

pub(crate) fn sample_time_bucket(sample_time: i64, start: i64, step: i64) -> i64 {
    if sample_time <= start {
        return start;
    }
    let offset = sample_time - start;
    start + (offset / step) * step
}

pub(crate) fn add_detected_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    name: &str,
    value: String,
    ty: DetectedFieldType,
    parser: &'static str,
) {
    fields
        .entry(name.to_string())
        .and_modify(|stats| stats.add(ty, value.clone(), parser))
        .or_insert_with(|| DetectedFieldStats::new(ty, value, parser));
}

pub(crate) fn add_generated_detected_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    name: &str,
    value: String,
    ty: DetectedFieldType,
) {
    fields
        .entry(name.to_string())
        .and_modify(|stats| stats.add_generated(ty, value.clone()))
        .or_insert_with(|| DetectedFieldStats::new_generated(ty, value));
}

pub(crate) fn detect_json_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(line) else {
        return;
    };
    for entry in object {
        let name = entry.0;
        let json_value = entry.1;
        let Some(value) = detected_json_value_string(&json_value) else {
            continue;
        };
        add_detected_field(
            fields,
            &name,
            value,
            field_type_from_json(&json_value),
            "json",
        );
    }
}

/// Records each structured-metadata pair as a field.
///
/// No parser produced it, so it names none: Loki 3.5.1 reports such a field
/// with `"parsers": null`, as it does `detected_level`.
pub(crate) fn detect_structured_metadata_fields(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    metadata: &Labels,
) {
    for entry in metadata {
        add_generated_detected_field(
            fields,
            entry.0,
            entry.1.clone(),
            field_type_from_str(entry.1),
        );
    }
}

pub(crate) fn field_type_from_json(value: &Value) -> DetectedFieldType {
    match value {
        Value::Bool(_) => DetectedFieldType::Boolean,
        Value::Number(number) => {
            if number.is_i64() || number.is_u64() {
                DetectedFieldType::Int
            } else {
                DetectedFieldType::Float
            }
        }
        Value::String(value) => field_type_from_str(value),
        Value::Null | Value::Array(_) | Value::Object(_) => DetectedFieldType::String,
    }
}

pub(crate) fn field_type_from_str(value: &str) -> DetectedFieldType {
    let normalized = value.to_ascii_lowercase();
    if matches!(normalized.as_str(), "true" | "false") {
        return DetectedFieldType::Boolean;
    }
    if value.parse::<i64>().is_ok() {
        return DetectedFieldType::Int;
    }
    if value.parse::<f64>().is_ok() {
        return DetectedFieldType::Float;
    }
    if is_prometheus_duration_literal(value) {
        return DetectedFieldType::Duration;
    }
    if is_bytes_literal(value) {
        return DetectedFieldType::Bytes;
    }
    DetectedFieldType::String
}

pub(crate) fn is_bytes_literal(value: &str) -> bool {
    let unit_start = value
        .find(|ch: char| ch.is_ascii_alphabetic())
        .unwrap_or(value.len());
    // No early return for a value with no letters: the unit is then the
    // empty string, which `detected_bytes_unit` already refuses.
    let Ok(amount) = value[..unit_start].parse::<f64>() else {
        return false;
    };
    amount.is_finite() && amount >= 0.0 && detected_bytes_unit(&value[unit_start..]).is_some()
}

pub(crate) async fn collect_detected_fields(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    params: &DetectedFieldsParams,
) -> Result<BTreeMap<String, DetectedFieldStats>, HttpQueryError> {
    let planned = crate::querier::analytics::authorize_and_prepare_analytics_read(
        state,
        security,
        headers,
        TenantErrorSurface::QuerierRead,
        params.start,
        params.end,
        &params.query,
        VolumeRangeLimit::Check,
    )
    .await?;
    let read = planned.read(RangeEnd::Inclusive);

    let mut fields = BTreeMap::new();
    let mut scanned_lines = 0_usize;
    for block in &planned.plan.blocks {
        if scanned_lines >= params.line_limit {
            break;
        }
        let Some(rows) = read_planned_log_block(&planned.state, &block.key).await? else {
            continue;
        };
        for row in rows {
            if scanned_lines >= params.line_limit {
                break;
            }
            if !reads_planned_row(&read, &row)? {
                continue;
            }
            scanned_lines += 1;
            detect_entry_fields(&mut fields, &row.line, &row.structured_metadata);
        }
    }

    for record in plan_hot_tail_records(&planned.state, &planned.plan) {
        if scanned_lines >= params.line_limit {
            break;
        }
        if is_deleted_log_entry(
            read.delete_filters,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) || !planned.plan.query.matches_with_fields(
            &record.labels,
            &record.line,
            &record.structured_metadata,
        ) {
            continue;
        }
        scanned_lines += 1;
        detect_entry_fields(&mut fields, &record.line, &record.structured_metadata);
    }

    Ok(fields)
}

/// Records every field one log entry shows: its structured
/// metadata, and what the JSON and logfmt parsers find in its line.
pub(crate) fn detect_entry_fields(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    line: &str,
    structured_metadata: &Labels,
) {
    detect_structured_metadata_fields(fields, structured_metadata);
    detect_json_fields(fields, line);
    detect_logfmt_fields(fields, line);
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{BTreeMap, Labels, detect_entry_fields};

    #[test]
    fn discovery_reports_existing_metadata_without_synthesizing_levels() {
        let mut fields = BTreeMap::new();
        detect_entry_fields(&mut fields, "plain error=none", &Labels::new());
        assert!(!fields.contains_key("detected_level"));
        let mut metadata = Labels::new();
        metadata.insert("detected_level".into(), "debug".into());
        detect_entry_fields(&mut fields, "level=debug error=none", &metadata);
        assert!(
            fields["detected_level"]
                .values
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                == ["debug"]
        );
        assert!(fields["detected_level"].parsers.is_empty());
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetectedDurationUnit {
    pub(crate) order: u8,
    pub(crate) bit: u16,
}

pub(crate) fn detected_duration_unit(unit: &str) -> Option<DetectedDurationUnit> {
    match unit {
        "y" => Some(DetectedDurationUnit {
            order: 0,
            bit: 1 << 0,
        }),
        "w" => Some(DetectedDurationUnit {
            order: 1,
            bit: 1 << 1,
        }),
        "d" => Some(DetectedDurationUnit {
            order: 2,
            bit: 1 << 2,
        }),
        "h" => Some(DetectedDurationUnit {
            order: 3,
            bit: 1 << 3,
        }),
        "m" => Some(DetectedDurationUnit {
            order: 4,
            bit: 1 << 4,
        }),
        "s" => Some(DetectedDurationUnit {
            order: 5,
            bit: 1 << 5,
        }),
        "ms" => Some(DetectedDurationUnit {
            order: 6,
            bit: 1 << 6,
        }),
        "us" => Some(DetectedDurationUnit {
            order: 7,
            bit: 1 << 7,
        }),
        "ns" => Some(DetectedDurationUnit {
            order: 8,
            bit: 1 << 8,
        }),
        _ => None,
    }
}

pub(crate) async fn execute_index_volume_query(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    kind: VolumeKind,
) -> Result<Value, HttpQueryError> {
    let params = parse_volume_params(raw_query)?;
    let (tenant, planned) = crate::querier::analytics::authorize_and_plan_analytics_query(
        state,
        security,
        headers,
        TenantErrorSurface::Read,
        params.start,
        params.end,
        &params.query,
        VolumeRangeLimit::Check,
    )
    .await?;
    let state = planned.state;
    let plan = planned.plan;
    let tenant = tenant.as_str();
    let volumes = index_volume_samples(&state, tenant, &plan, &params);
    let mut response = match kind {
        VolumeKind::Instant => loki_volume_vector_response(volumes, params.end, params.limit),
        VolumeKind::Range => {
            if params.step.is_some_and(|step| step <= 0) {
                return Err(HttpQueryError::InvalidStep);
            }
            // Loki writes an empty `volume_range` answer as a `vector`, and
            // one with series in it as a `matrix`.
            if volumes.is_empty() {
                loki_volume_vector_response(volumes, params.end, params.limit)
            } else {
                loki_volume_matrix_response(volumes, params.limit)
            }
        }
    };
    // A volume read scans no lines, and Loki's stats for one say so. The stats
    // are built from an empty answer, so a `matrix`'s samples are not counted
    // as lines processed.
    response["data"]["stats"] =
        add_loki_query_stats_for_stream_plan(json!({}), &plan)["data"]["stats"].take();
    Ok(response)
}

pub(crate) fn index_volume_samples(
    state: &QuerierState,
    tenant: &str,
    plan: &StreamPlan,
    params: &VolumeParams,
) -> BTreeMap<Labels, BTreeMap<i64, u64>> {
    let mut volumes = BTreeMap::<Labels, BTreeMap<i64, u64>>::new();
    for block in &plan.blocks {
        let matching_fingerprints = block
            .fingerprints
            .iter()
            .filter(|fingerprint| plan.fingerprints.contains(fingerprint))
            .copied()
            .collect::<Vec<_>>();
        if matching_fingerprints.is_empty() {
            continue;
        }

        let sample_time = block.key.time_range.start_ns.max(plan.time_range.start_ns);
        for fingerprint in matching_fingerprints {
            let Some(labels) = state.label_index.labels_for(tenant, fingerprint) else {
                continue;
            };
            for metric in volume_metrics_for_labels(labels, params) {
                let samples = volumes.entry(metric).or_default();
                let sample = samples.entry(sample_time).or_default();
                *sample = sample.saturating_add(block.size.bytes_u64());
            }
        }
    }

    // A record still in the WAL counts its line toward the step it falls in.
    for record in plan_hot_tail_records(state, plan) {
        let sample_time = params
            .step
            .filter(|step| *step > 0)
            .map_or(record.timestamp_ns.max(plan.time_range.start_ns), |step| {
                sample_time_bucket(record.timestamp_ns, plan.time_range.start_ns, step)
            });
        let bytes = u64::try_from(record.line.len()).unwrap_or(u64::MAX);
        for metric in volume_metrics_for_labels(&record.labels, params) {
            let samples = volumes.entry(metric).or_default();
            let sample = samples.entry(sample_time).or_default();
            *sample = sample.saturating_add(bytes);
        }
    }
    volumes
}

pub(crate) fn is_prometheus_duration_literal(value: &str) -> bool {
    let mut pos = 0;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;

    while pos < value.len() {
        let value_start = pos;
        while value.as_bytes().get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        if pos == value_start {
            return false;
        }

        let unit_start = pos;
        while value
            .as_bytes()
            .get(pos)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            pos += 1;
        }
        let Some(unit) = detected_duration_unit(&value[unit_start..pos]) else {
            return false;
        };
        if previous_unit_order.is_some_and(|previous| unit.order <= previous) {
            return false;
        }

        previous_unit_order = Some(unit.order);
        parsed_chunk = true;
    }

    parsed_chunk
}

pub(crate) struct LimitedVolumeSeries {
    pub(crate) labels: Labels,
    pub(crate) samples: BTreeMap<i64, u64>,
}

pub(crate) fn limit_volume_series(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    limit: usize,
) -> Vec<LimitedVolumeSeries> {
    volumes
        .into_iter()
        .take(limit)
        .map(|entry| LimitedVolumeSeries {
            labels: entry.0,
            samples: entry.1,
        })
        .collect()
}

/// The `index/volume_range` answer when it holds series: one series per
/// metric, one sample per step that holds bytes.
///
/// Loki answers such a `volume_range` with a `matrix`. It answers an empty one,
/// and every `index/volume`, with a `vector`.
pub(crate) fn loki_volume_matrix_response(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    limit: usize,
) -> Value {
    let result = limit_volume_series(volumes, limit)
        .into_iter()
        .map(|series| {
            let values = series
                .samples
                .into_iter()
                .map(|sample| {
                    json!([
                        unix_ns_string_to_loki_seconds(&sample.0.to_string()),
                        sample.1.to_string()
                    ])
                })
                .collect::<Vec<_>>();
            json!({
                "metric": series.labels,
                "values": values,
            })
        })
        .collect::<Vec<_>>();

    loki_success_value(json!({
        "resultType": "matrix",
        "result": result,
    }))
}

pub(crate) fn loki_volume_vector_response(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    timestamp: i64,
    limit: usize,
) -> Value {
    let timestamp = unix_ns_string_to_loki_seconds(&timestamp.to_string());
    let result = limit_volume_series(volumes, limit)
        .into_iter()
        .map(|series| {
            let value = series
                .samples
                .values()
                .copied()
                .fold(0_u64, u64::saturating_add);
            json!({
                "metric": series.labels,
                "value": [timestamp, value.to_string()],
            })
        })
        .collect::<Vec<_>>();

    loki_success_value(json!({
        "resultType": "vector",
        "result": result,
    }))
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ParsedLogfmtPair {
    pub(crate) name: String,
    pub(crate) value: String,
}

pub(crate) fn parse_logfmt_pairs(line: &str) -> Vec<ParsedLogfmtPair> {
    let bytes = line.as_bytes();
    let mut pairs = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let key_start = index;
        while index < bytes.len() && !bytes[index].is_ascii_whitespace() && bytes[index] != b'=' {
            index += 1;
        }
        if key_start == index || index >= bytes.len() || bytes[index] != b'=' {
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            continue;
        }
        let key = &line[key_start..index];
        index += 1;
        let value = if index < bytes.len() && bytes[index] == b'"' {
            index += 1;
            let mut value = String::new();
            while index < bytes.len() {
                match bytes[index] {
                    b'\\' if index + 1 < bytes.len() => {
                        index += 1;
                        value.push(bytes[index] as char);
                        index += 1;
                    }
                    b'"' => {
                        index += 1;
                        break;
                    }
                    byte => {
                        value.push(byte as char);
                        index += 1;
                    }
                }
            }
            value
        } else {
            let value_start = index;
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            line[value_start..index].to_string()
        };
        pairs.push(ParsedLogfmtPair {
            name: key.to_string(),
            value,
        });
    }
    pairs
}

/// Whether an analytics read is also held to the volume endpoints' range
/// limit.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum VolumeRangeLimit {
    Check,
    Skip,
}

/// An index analytics read of `query` over `time_range` for `tenant`.
pub(crate) struct AnalyticsQuery<'a> {
    pub(crate) tenant: &'a TenantId,
    pub(crate) time_range: TimeRange,
    pub(crate) query: &'a str,
    pub(crate) volume_range_limit: VolumeRangeLimit,
}

pub(crate) struct PlannedAnalyticsQuery {
    pub(crate) state: QuerierState,
    pub(crate) plan: StreamPlan,
    pub(crate) time_range: TimeRange,
}

/// Plans an index analytics read, and checks it against the tenant's limits.
///
/// Returns the request's state, the plan, and the clamped time range.
pub(crate) async fn plan_analytics_query(
    state: &QuerierState,
    request: AnalyticsQuery<'_>,
) -> Result<PlannedAnalyticsQuery, HttpQueryError> {
    let AnalyticsQuery {
        tenant,
        time_range,
        query,
        volume_range_limit,
    } = request;
    // One resolution for the whole request: every check below reads the
    // tenant's limits from this state.
    let state = &state.with_tenant_limits(tenant);
    let tenant = tenant.as_str();
    let time_range = clamp_query_lookback(&state.limits, time_range, current_unix_time_ns());
    if volume_range_limit == VolumeRangeLimit::Check {
        validate_loki_volume_query_range_limit(state, time_range)?;
    }
    validate_query_range_limit(state, time_range)?;
    validate_query_string_bytes_limit(state, query)?;
    let state = state.with_request_tenant_index(tenant, time_range).await?;
    let parsed = parse_query(query).map_err(|source| HttpQueryError::LokiParse {
        query: query.to_string(),
        source,
    })?;
    let plan = plan_stream_query(
        tenant,
        time_range,
        parsed,
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    Ok(PlannedAnalyticsQuery {
        state,
        plan,
        time_range,
    })
}

/// The hot-tail records a stream plan selects: the plan's tenant, inside its
/// window, not yet compacted into a block, and matching its label matchers.
///
/// A block answers for what the block builder already wrote. The rest is still
/// in the WAL, and Loki answers for it from the ingester, so an analytics read
/// that skipped this reported nothing about the last minutes of logs.
/// The pipeline stages are not applied here. A caller that needs them runs
/// them.
pub(crate) fn plan_hot_tail_records(state: &QuerierState, plan: &StreamPlan) -> Vec<WalLogRecord> {
    let Some(hot_tail) = &state.hot_tail else {
        return Vec::new();
    };
    let frontier = hot_tail.frontier.snapshot();
    let mut records = hot_tail
        .source
        .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
    records.retain(|record| {
        record.tenant == plan.tenant
            && !frontier.is_compacted(record)
            && plan.time_range.start_ns <= record.timestamp_ns
            && record.timestamp_ns <= plan.time_range.end_ns
            && plan
                .query
                .matchers
                .iter()
                .all(|matcher| matcher.matches(&record.labels))
    });
    records
}

/// Whether a planned read's range includes a row at its end.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeEnd {
    Inclusive,
    Exclusive,
}

/// An analytics read under `plan`, with the delete requests that remove rows
/// from it.
pub(crate) struct PlannedRead<'a> {
    pub(crate) state: &'a QuerierState,
    pub(crate) plan: &'a StreamPlan,
    pub(crate) delete_filters: &'a [ActiveLogDeleteFilter],
    pub(crate) end: RangeEnd,
}

/// Whether `read` reads a stored `row`: the row is in the plan's series and
/// time range, no delete request removes it, and it matches the plan's query.
pub(crate) fn reads_planned_row(read: &PlannedRead<'_>, row: &LogRow) -> Result<bool, QueryError> {
    let PlannedRead {
        state,
        plan,
        delete_filters,
        end,
    } = *read;
    let past_end = match end {
        RangeEnd::Exclusive => row.timestamp_ns >= plan.time_range.end_ns,
        RangeEnd::Inclusive => row.timestamp_ns > plan.time_range.end_ns,
    };
    if !plan.fingerprints.contains(&row.series_fingerprint)
        || row.timestamp_ns < plan.time_range.start_ns
        || past_end
    {
        return Ok(false);
    }
    let tenant = plan.tenant.as_str();
    let labels = state
        .label_index
        .labels_for(tenant, row.series_fingerprint)
        .ok_or(QueryError::MissingSeriesLabels {
            tenant: tenant.to_string(),
            fingerprint: row.series_fingerprint,
        })?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        &row.line,
        &row.structured_metadata,
        row.timestamp_ns,
    ) {
        return Ok(false);
    }
    Ok(plan
        .query
        .matches_with_fields(labels, &row.line, &row.structured_metadata))
}

fn labels_aggregate_by_name(name: &str) -> Labels {
    BTreeMap::from([(name.to_string(), String::new())])
}

pub(crate) fn volume_metrics_for_labels(labels: &Labels, params: &VolumeParams) -> Vec<Labels> {
    match params.aggregate_by {
        VolumeAggregateBy::Series => {
            let labels = if let Some(target_labels) = &params.target_labels {
                project_labels(labels, target_labels)
            } else {
                labels.clone()
            };
            vec![labels]
        }
        VolumeAggregateBy::Labels => match &params.target_labels {
            Some(target_labels) => target_labels
                .iter()
                .filter(|name| labels.contains_key(*name))
                .map(|name| labels_aggregate_by_name(name))
                .collect(),
            None => labels
                .keys()
                .map(|name| labels_aggregate_by_name(name))
                .collect(),
        },
    }
}
