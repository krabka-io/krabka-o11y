use super::*;

pub(crate) async fn collect_detected_fields(
    state: &QuerierState,
    headers: &HeaderMap,
    params: &DetectedFieldsParams,
) -> Result<BTreeMap<String, DetectedFieldStats>, HttpQueryError> {
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
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
    let delete_filters = active_log_delete_filters(&state, tenant, time_range)?;

    let mut fields = BTreeMap::new();
    let mut scanned_lines = 0_usize;
    for block in &plan.blocks {
        if scanned_lines >= params.line_limit {
            break;
        }
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
            if scanned_lines >= params.line_limit {
                break;
            }
            if !plan.fingerprints.contains(&row.series_fingerprint)
                || row.timestamp_ns < plan.time_range.start_ns
                || row.timestamp_ns > plan.time_range.end_ns
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
            scanned_lines += 1;
            detect_detected_level_field(&mut fields, labels, &row.line);
            detect_structured_metadata_fields(&mut fields, &row.structured_metadata);
            detect_json_fields(&mut fields, &row.line);
            detect_logfmt_fields(&mut fields, &row.line);
        }
    }

    Ok(fields)
}

pub(crate) fn detect_detected_level_field(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    labels: &Labels,
    line: &str,
) {
    if !should_insert_unknown_detected_level(labels) {
        return;
    }
    let level = detect_log_level(line).unwrap_or("unknown");
    add_generated_detected_field(
        fields,
        "detected_level",
        level.to_string(),
        DetectedFieldType::String,
    );
}

pub(crate) fn detect_structured_metadata_fields(
    fields: &mut BTreeMap<String, DetectedFieldStats>,
    metadata: &Labels,
) {
    for (name, value) in metadata {
        add_detected_field(
            fields,
            name,
            value.clone(),
            field_type_from_str(value),
            "structured_metadata",
        );
    }
}

pub(crate) fn detect_json_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(line) else {
        return;
    };
    for (name, json_value) in object {
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

pub(crate) fn detect_logfmt_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    for (name, value) in parse_logfmt_pairs(line) {
        let ty = field_type_from_str(&value);
        add_detected_field(fields, &name, value, ty, "logfmt");
    }
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

pub(crate) fn detected_json_value_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) => Some(value.clone()),
        Value::Array(_) | Value::Object(_) => Some(value.to_string()),
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
        let Some((unit_order, _)) = detected_duration_unit(&value[unit_start..pos]) else {
            return false;
        };
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return false;
        }

        previous_unit_order = Some(unit_order);
        parsed_chunk = true;
    }

    parsed_chunk
}

pub(crate) fn detected_duration_unit(unit: &str) -> Option<(u8, u16)> {
    match unit {
        "y" => Some((0, 1 << 0)),
        "w" => Some((1, 1 << 1)),
        "d" => Some((2, 1 << 2)),
        "h" => Some((3, 1 << 3)),
        "m" => Some((4, 1 << 4)),
        "s" => Some((5, 1 << 5)),
        "ms" => Some((6, 1 << 6)),
        "us" => Some((7, 1 << 7)),
        "ns" => Some((8, 1 << 8)),
        _ => None,
    }
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

pub(crate) fn detected_bytes_unit(unit: &str) -> Option<()> {
    match unit {
        "B" | "kB" | "KB" | "MB" | "GB" | "TB" | "KiB" | "MiB" | "GiB" | "TiB" => Some(()),
        _ => None,
    }
}

pub(crate) fn parse_logfmt_pairs(line: &str) -> Vec<(String, String)> {
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
        pairs.push((key.to_string(), value));
    }
    pairs
}

pub(crate) async fn execute_index_volume_query(
    state: &QuerierState,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    kind: VolumeKind,
) -> Result<Value, HttpQueryError> {
    let params = parse_volume_params(raw_query)?;
    let tenant = authorized_tenant(state, headers).await?;
    let time_range = TimeRange::new(params.start, params.end)?;
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
    let volumes = index_volume_samples(&state, tenant, &plan, &params);
    let response = match kind {
        VolumeKind::Instant => loki_volume_vector_response(volumes, params.end, params.limit),
        VolumeKind::Range => {
            if params.step.is_some_and(|step| step <= 0) {
                return Err(HttpQueryError::InvalidStep);
            }
            loki_volume_vector_response(volumes, params.end, params.limit)
        }
    };
    Ok(add_loki_query_stats_for_stream_plan(response, &plan))
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
    volumes
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
                .map(|name| BTreeMap::from([(name.clone(), String::new())]))
                .collect(),
            None => labels
                .keys()
                .map(|name| BTreeMap::from([(name.clone(), String::new())]))
                .collect(),
        },
    }
}

pub(crate) fn project_labels(labels: &Labels, target_labels: &[String]) -> Labels {
    target_labels
        .iter()
        .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
        .collect()
}

pub(crate) fn loki_volume_vector_response(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    timestamp: i64,
    limit: usize,
) -> Value {
    let result = limit_volume_series(volumes, limit)
        .into_iter()
        .map(|(metric, samples)| {
            let value = samples.values().copied().fold(0_u64, u64::saturating_add);
            json!({
                "metric": metric,
                "value": [timestamp, value.to_string()],
            })
        })
        .collect::<Vec<_>>();

    loki_success_value(json!({
        "resultType": "vector",
        "result": result,
    }))
}

pub(crate) fn limit_volume_series(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    limit: usize,
) -> Vec<(Labels, BTreeMap<i64, u64>)> {
    volumes.into_iter().take(limit).collect()
}

pub(crate) fn sample_time_bucket(sample_time: i64, start: i64, step: i64) -> i64 {
    if sample_time <= start {
        return start;
    }
    let offset = sample_time - start;
    start + (offset / step) * step
}
