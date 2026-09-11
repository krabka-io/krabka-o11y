use super::{
    DistributorError, Limits, LokiProtoPushRequest, TenantId, WalLogRecord,
    discover_service_name_label, loki_missing_proto_timestamp_error,
    loki_proto_label_pairs_to_labels, loki_proto_timestamp_ns, loki_push_entry_labels,
    parse_loki_proto_labels, validate_loki_line_size, validate_loki_stream_labels,
    validate_loki_timestamp_window,
};

pub(crate) fn normalize_loki_proto_push(
    tenant: &TenantId,
    payload: LokiProtoPushRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for stream in payload.streams {
        let mut stream_labels = parse_loki_proto_labels(&stream.labels)?;
        validate_loki_stream_labels(&stream_labels, limits)?;
        discover_service_name_label(&mut stream_labels);

        for entry in stream.entries {
            let timestamp_ns = if let Some(timestamp) = entry.timestamp.as_ref() {
                loki_proto_timestamp_ns(Some(timestamp))?
            } else {
                return Err(loki_missing_proto_timestamp_error(
                    &stream_labels,
                    limits.reject_old_samples_max_age,
                ));
            };
            validate_loki_timestamp_window(timestamp_ns, &stream_labels, limits)?;
            validate_loki_line_size(&entry.line, &stream_labels, limits)?;
            let labels = loki_push_entry_labels(&stream_labels, &entry.line);
            records.push(WalLogRecord {
                tenant: tenant.to_owned(),
                labels,
                timestamp_ns,
                line: entry.line,
                structured_metadata: loki_proto_label_pairs_to_labels(&entry.structured_metadata),
                position: None,
            });
        }
    }

    if records.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }

    Ok(records)
}
