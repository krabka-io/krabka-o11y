use super::{
    DistributorError, Limits, LokiTypedPushRequest, TenantId, Value, WalLogRecord,
    discover_service_name_label, loki_json_line_parse_error, loki_json_timestamp_parse_error,
    loki_push_entry_labels, parse_structured_metadata, validate_ingest_timestamp_ns,
    validate_loki_empty_json_value_timestamp_window, validate_loki_line_size,
    validate_loki_stream_labels, validate_loki_timestamp_window,
};

pub(crate) fn normalize_loki_push(
    tenant: &TenantId,
    payload: LokiTypedPushRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for stream in payload.streams {
        let Some(original_stream_labels) = stream.stream else {
            continue;
        };
        validate_loki_stream_labels(&original_stream_labels, limits)?;
        let mut stream_labels = original_stream_labels.clone();
        discover_service_name_label(&mut stream_labels);

        let Some(values) = stream.values else {
            continue;
        };
        for value in values {
            let Some(value) = value.as_array() else {
                return Err(DistributorError::InvalidPushValue);
            };
            let zero_timestamp;
            let (timestamp, line, metadata, is_empty_value) = match value.as_slice() {
                [timestamp] => (timestamp, "", [].as_slice(), false),
                [timestamp, line, metadata @ ..] => (
                    timestamp,
                    line.as_str().ok_or_else(|| {
                        DistributorError::InvalidJsonLineSyntax(loki_json_line_parse_error(
                            &original_stream_labels,
                            timestamp.as_str().unwrap_or_default(),
                            line,
                        ))
                    })?,
                    metadata,
                    false,
                ),
                [] => {
                    zero_timestamp = Value::String("0".to_string());
                    (&zero_timestamp, "", [].as_slice(), true)
                }
            };
            let timestamp = timestamp
                .as_str()
                .ok_or(DistributorError::InvalidTimestamp)?;
            let timestamp_ns = timestamp.parse().map_err(|_| {
                DistributorError::InvalidJsonTimestampSyntax(loki_json_timestamp_parse_error(
                    timestamp, line,
                ))
            })?;
            let timestamp_ns = validate_ingest_timestamp_ns(timestamp_ns)?;
            if is_empty_value {
                validate_loki_empty_json_value_timestamp_window(
                    &stream_labels,
                    limits.reject_old_samples_max_age,
                )?;
            }
            validate_loki_timestamp_window(timestamp_ns, &stream_labels, limits)?;
            validate_loki_line_size(line, &stream_labels, limits)?;
            let labels = loki_push_entry_labels(&stream_labels, line);

            records.push(WalLogRecord {
                tenant: tenant.to_owned(),
                labels,
                timestamp_ns,
                line: line.to_string(),
                structured_metadata: parse_structured_metadata(metadata.first())?,
                position: None,
            });
        }
    }

    Ok(records)
}
