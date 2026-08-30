use super::*;

pub(crate) fn decode_native_kafka_log_record(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    let tenant = required_kafka_header_utf8(&record.headers, "krabka-tenant")?;
    let timestamp_ns = if let Some(value) =
        optional_kafka_header_utf8(&record.headers, "krabka-log-timestamp-ns")?
    {
        let timestamp_ns =
            value
                .parse()
                .map_err(|source| WalRecordDecodeError::InvalidNativeTimestamp {
                    value: value.clone(),
                    source,
                })?;
        validate_native_timestamp_ns(timestamp_ns, value)?
    } else {
        let timestamp_ms =
            record
                .timestamp_ms
                .ok_or_else(|| WalRecordDecodeError::MissingNativeHeader {
                    name: "krabka-log-timestamp-ns".to_string(),
                })?;
        native_timestamp_ms_to_ns(timestamp_ms)?
    };
    let labels = kafka_headers_with_prefix(&record.headers, "krabka-log-label-", |name| {
        WalRecordDecodeError::DuplicateNativeLabelName { name }
    })?;
    if labels.is_empty() {
        return Err(WalRecordDecodeError::MissingNativeLabels);
    }
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(WalRecordDecodeError::InvalidNativeLabelName { name: name.clone() });
    }
    let structured_metadata =
        kafka_headers_with_prefix(&record.headers, "krabka-log-metadata-", |name| {
            WalRecordDecodeError::DuplicateNativeMetadataName { name }
        })?;
    if let Some(name) = structured_metadata
        .keys()
        .find(|name| !is_loki_label_name(name))
    {
        return Err(WalRecordDecodeError::InvalidNativeMetadataName { name: name.clone() });
    }
    let line = String::from_utf8(record.value)
        .map_err(|_| WalRecordDecodeError::InvalidNativeLogLineUtf8)?;

    Ok(WalLogRecord {
        tenant,
        labels,
        timestamp_ns,
        line,
        structured_metadata,
        position: Some(WalPosition {
            partition: record.partition,
            offset: record.offset,
        }),
    })
}
