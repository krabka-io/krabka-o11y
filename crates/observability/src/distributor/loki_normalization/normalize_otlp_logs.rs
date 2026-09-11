use super::{
    DistributorError, Limits, OtlpLogsRequest, TenantId, WalLogRecord, discover_service_name_label,
    otlp_attributes_to_labels, otlp_log_record_structured_metadata, otlp_timestamp_ns,
    otlp_value_to_string, validate_loki_label_limits, validate_loki_line_size,
    validate_loki_timestamp_window,
};

pub(crate) fn normalize_otlp_logs(
    tenant: &TenantId,
    payload: OtlpLogsRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_labels = otlp_attributes_to_labels(
            resource_logs
                .resource
                .as_ref()
                .and_then(|resource| resource.attributes.as_deref()),
        )?;

        for scope_logs in resource_logs.scope_logs {
            let mut labels = resource_labels.clone();
            labels.extend(otlp_attributes_to_labels(
                scope_logs
                    .scope
                    .as_ref()
                    .and_then(|scope| scope.attributes.as_deref()),
            )?);
            discover_service_name_label(&mut labels);
            if labels.is_empty() {
                return Err(DistributorError::EmptyStreamLabels);
            }
            // The OTLP paths do not check the label *syntax*: the attribute
            // names come through `otlp_attributes_to_labels`, which already
            // shapes them. The caps are a different question and apply here.
            validate_loki_label_limits(&labels, limits)?;

            for log_record in scope_logs.log_records {
                let timestamp_ns = otlp_timestamp_ns(&log_record.time_unix_nano)?;
                validate_loki_timestamp_window(timestamp_ns, &labels, limits)?;
                let line = log_record
                    .body
                    .as_ref()
                    .map(otlp_value_to_string)
                    .unwrap_or_default();
                validate_loki_line_size(&line, &labels, limits)?;
                records.push(WalLogRecord {
                    tenant: tenant.to_owned(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line,
                    structured_metadata: otlp_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}
