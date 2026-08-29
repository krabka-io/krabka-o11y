use super::*;

pub(crate) fn loki_proto_timestamp_ns(
    timestamp: Option<&LokiProtoTimestamp>,
) -> Result<i64, DistributorError> {
    let timestamp = timestamp.ok_or(DistributorError::InvalidTimestamp)?;
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(DistributorError::InvalidTimestamp);
    }

    timestamp
        .seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds_ns| seconds_ns.checked_add(i64::from(timestamp.nanos)))
        .ok_or(DistributorError::InvalidTimestamp)
}

pub(crate) fn loki_missing_proto_timestamp_error(
    stream_labels: &Labels,
    max_age: Option<Time>,
) -> DistributorError {
    let max_age = max_age.unwrap_or(LOKI_REJECT_OLD_SAMPLES_MAX_AGE);
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    }
}

pub(crate) fn loki_proto_label_pairs_to_labels(labels: &[LokiProtoLabelPair]) -> Labels {
    let mut labels_by_name = Labels::new();
    for label in labels {
        labels_by_name.insert(label.name.clone(), label.value.clone());
    }
    labels_by_name
}

pub(crate) fn normalize_otlp_proto_logs(
    headers: &HeaderMap,
    payload: ProtoExportLogsServiceRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?;
    normalize_otlp_proto_logs_for_tenant(
        tenant,
        payload,
        reject_old_samples_max_age,
        creation_grace_period,
    )
}

pub(crate) fn normalize_otlp_proto_logs_for_tenant(
    tenant: &str,
    payload: ProtoExportLogsServiceRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.to_string();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_labels = proto_attributes_to_labels(
            resource_logs
                .resource
                .as_ref()
                .map(|resource| resource.attributes.as_slice()),
        )?;

        for scope_logs in resource_logs.scope_logs {
            let mut labels = resource_labels.clone();
            labels.extend(proto_attributes_to_labels(
                scope_logs
                    .scope
                    .as_ref()
                    .map(|scope| scope.attributes.as_slice()),
            )?);
            discover_service_name_label(&mut labels);
            if labels.is_empty() {
                return Err(DistributorError::EmptyStreamLabels);
            }

            for log_record in scope_logs.log_records {
                let timestamp_ns = proto_timestamp_ns(
                    log_record.time_unix_nano,
                    log_record.observed_time_unix_nano,
                )?;
                validate_loki_timestamp_window(
                    timestamp_ns,
                    &labels,
                    reject_old_samples_max_age,
                    creation_grace_period,
                )?;
                records.push(WalLogRecord {
                    tenant: tenant.clone(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(proto_value_to_string)
                        .unwrap_or_default(),
                    structured_metadata: proto_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}

pub(crate) fn otlp_attributes_to_labels(
    attributes: Option<&[OtlpKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        if attribute.key.is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let name = normalize_otlp_attribute_name(&attribute.key);
        if labels
            .insert(name, otlp_value_to_string(&attribute.value))
            .is_some()
        {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(labels)
}

pub(crate) fn proto_attributes_to_labels(
    attributes: Option<&[ProtoKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        if attribute.key.is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let name = normalize_otlp_attribute_name(&attribute.key);
        let value = attribute
            .value
            .as_ref()
            .map(proto_value_to_string)
            .unwrap_or_default();
        if labels.insert(name, value).is_some() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(labels)
}

pub(crate) fn proto_log_record_structured_metadata(
    log_record: &ProtoLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = proto_attributes_to_labels(Some(log_record.attributes.as_slice()))?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        (log_record.severity_number != 0).then(|| log_record.severity_number.to_string()),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        (!log_record.severity_text.is_empty()).then(|| log_record.severity_text.clone()),
    )?;
    insert_proto_trace_context_metadata(&mut metadata, "trace_id", &log_record.trace_id);
    insert_proto_trace_context_metadata(&mut metadata, "span_id", &log_record.span_id);
    Ok(metadata)
}

pub(crate) fn otlp_log_record_structured_metadata(
    log_record: &OtlpLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = otlp_attributes_to_labels(log_record.attributes.as_deref())?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        log_record
            .severity_number
            .as_ref()
            .map(otlp_severity_number_to_string)
            .transpose()?,
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        log_record
            .severity_text
            .as_ref()
            .filter(|severity_text| !severity_text.is_empty())
            .cloned(),
    )?;
    Ok(metadata)
}

pub(crate) fn insert_metadata_if_absent(
    metadata: &mut Labels,
    name: &str,
    value: Option<String>,
) -> Result<(), DistributorError> {
    let Some(value) = value else {
        return Ok(());
    };
    if metadata.insert(name.to_string(), value).is_some() {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    Ok(())
}

pub(crate) fn insert_proto_trace_context_metadata(metadata: &mut Labels, name: &str, value: &[u8]) {
    if !value.is_empty() {
        metadata.insert(name.to_string(), hex_string(value));
    }
}

pub(crate) fn normalize_otlp_attribute_name(name: &str) -> String {
    let mut normalized = name
        .chars()
        .map(|ch| {
            if ch == '_' || ch.is_ascii_alphanumeric() {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        normalized.push('_');
    }
    if normalized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        normalized.insert(0, '_');
    }
    normalized
}

pub(crate) fn discover_service_name_label(labels: &mut Labels) {
    if labels.contains_key("service_name") {
        return;
    }

    let service_name = SERVICE_NAME_DISCOVERY_LABELS
        .iter()
        .filter_map(|name| labels.get(*name))
        .find(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "unknown_service".to_string());
    labels.insert("service_name".to_string(), service_name);
}

pub(crate) fn discover_detected_level_label(labels: &mut Labels, line: &str) {
    if labels.contains_key("detected_level")
        || labels.contains_key("level")
        || labels.contains_key("severity")
        || labels.contains_key("severity_text")
    {
        return;
    }

    let level = detect_log_level(line);
    if let Some(level) = level {
        labels.insert("detected_level".to_string(), level.to_string());
    }
}

pub(crate) fn detect_log_level(line: &str) -> Option<&'static str> {
    let line = line.to_ascii_lowercase();
    for level in [
        "critical", "crit", "fatal", "error", "warn", "warning", "info", "debug", "trace",
    ] {
        if contains_log_level_token(&line, level) {
            return Some(match level {
                "crit" => "critical",
                "warning" => "warn",
                level => level,
            });
        }
    }
    None
}

pub(crate) fn contains_log_level_token(line: &str, level: &str) -> bool {
    line.match_indices(level).any(|(start, _)| {
        let end = start + level.len();
        let before = start
            .checked_sub(1)
            .and_then(|index| line.as_bytes().get(index))
            .copied();
        let after = line.as_bytes().get(end).copied();
        !before.is_some_and(is_log_level_word_byte) && !after.is_some_and(is_log_level_word_byte)
    })
}

pub(crate) fn is_log_level_word_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub(crate) const SERVICE_NAME_DISCOVERY_LABELS: &[&str] = &[
    "service",
    "app",
    "application",
    "name",
    "app_kubernetes_io_name",
    "container",
    "container_name",
    "component",
    "workload",
    "job",
];

pub(crate) fn proto_timestamp_ns(
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
) -> Result<i64, DistributorError> {
    let timestamp = if time_unix_nano == 0 {
        observed_time_unix_nano
    } else {
        time_unix_nano
    };
    i64::try_from(timestamp).map_err(|_| DistributorError::InvalidTimestamp)
}

pub(crate) fn otlp_timestamp_ns(timestamp: &Value) -> Result<i64, DistributorError> {
    let timestamp_ns = match timestamp {
        Value::String(timestamp) => timestamp
            .parse()
            .map_err(|_| DistributorError::InvalidTimestamp),
        Value::Number(timestamp) => timestamp.as_i64().ok_or(DistributorError::InvalidTimestamp),
        _ => Err(DistributorError::InvalidTimestamp),
    }?;
    validate_ingest_timestamp_ns(timestamp_ns)
}

pub(crate) fn validate_ingest_timestamp_ns(timestamp_ns: i64) -> Result<i64, DistributorError> {
    if timestamp_ns < 0 {
        Err(DistributorError::InvalidTimestamp)
    } else {
        Ok(timestamp_ns)
    }
}

pub(crate) fn validate_loki_timestamp_window(
    timestamp_ns: i64,
    stream_labels: &Labels,
    max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<(), DistributorError> {
    validate_loki_timestamp_window_at(
        timestamp_ns,
        current_unix_time_ns(),
        stream_labels,
        max_age,
        creation_grace_period,
    )
}

/// The window check against a caller-supplied `now`.
///
/// Split out so the two bounds can be tested exactly at their edges. Both are
/// strict comparisons -- a timestamp precisely at the oldest or newest
/// acceptable value is accepted -- and against a wall clock that boundary is
/// unreachable: `now` advances between choosing the timestamp and reading it.
pub(crate) fn validate_loki_timestamp_window_at(
    timestamp_ns: i64,
    now_ns: i64,
    stream_labels: &Labels,
    max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<(), DistributorError> {
    if let Some(max_age) = max_age {
        let oldest_acceptable_timestamp_ns = now_ns.saturating_sub(max_age.nanos_i64());
        if timestamp_ns < oldest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooOld {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
                oldest_acceptable_timestamp_ns,
            });
        }
    }
    if let Some(creation_grace_period) = creation_grace_period {
        let newest_acceptable_timestamp_ns =
            now_ns.saturating_add(creation_grace_period.nanos_i64());
        if timestamp_ns > newest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooNew {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
            });
        }
    }
    Ok(())
}

pub(crate) fn loki_stale_sample_label_set(labels: &Labels) -> String {
    let values = labels
        .iter()
        .map(|(name, value)| format!("{name}={}", quote_logql_string(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{values}}}")
}

pub(crate) fn rfc3339_seconds(timestamp_ns: i64) -> String {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(seconds) else {
        return seconds.to_string();
    };
    let date = timestamp.date();
    let time = timestamp.time();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        date.year(),
        u8::from(date.month()),
        date.day(),
        time.hour(),
        time.minute(),
        time.second()
    )
}

pub(crate) fn otlp_severity_number_to_string(value: &Value) -> Result<String, DistributorError> {
    match value {
        Value::Number(number) => Ok(number.to_string()),
        Value::String(string) => Ok(string.clone()),
        _ => Err(DistributorError::InvalidOtlpPayload),
    }
}

pub(crate) fn otlp_value_to_string(value: &OtlpAnyValue) -> String {
    match value {
        OtlpAnyValue::String(value) | OtlpAnyValue::Bytes(value) => value.clone(),
        OtlpAnyValue::Bool(value) => value.to_string(),
        OtlpAnyValue::Int(value) | OtlpAnyValue::Double(value) => metadata_value_to_string(value),
        OtlpAnyValue::Array(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(otlp_value_to_json)
                .collect::<Vec<_>>(),
        )
        .expect("OTLP array values serialize to JSON"),
        OtlpAnyValue::Kvlist(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(|attribute| (attribute.key.clone(), otlp_value_to_json(&attribute.value)))
                .collect::<BTreeMap<_, _>>(),
        )
        .expect("OTLP key-value lists serialize to JSON"),
    }
}
