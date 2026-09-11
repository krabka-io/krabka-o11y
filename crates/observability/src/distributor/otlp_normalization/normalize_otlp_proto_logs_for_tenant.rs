use super::{
    DistributorError, Limits, ProtoExportLogsServiceRequest, TenantId, WalLogRecord,
    discover_service_name_label, proto_attributes_to_labels, proto_log_record_structured_metadata,
    proto_timestamp_ns, proto_value_to_string, validate_loki_label_limits, validate_loki_line_size,
    validate_loki_timestamp_window,
};

pub(crate) fn normalize_otlp_proto_logs_for_tenant(
    tenant: &TenantId,
    payload: ProtoExportLogsServiceRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
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
            // As in the OTLP/JSON path: the caps apply, the name syntax check
            // does not, because these names came through
            // `proto_attributes_to_labels`.
            validate_loki_label_limits(&labels, limits)?;

            for log_record in scope_logs.log_records {
                let timestamp_ns = proto_timestamp_ns(
                    log_record.time_unix_nano,
                    log_record.observed_time_unix_nano,
                )?;
                validate_loki_timestamp_window(timestamp_ns, &labels, limits)?;
                let line = log_record
                    .body
                    .as_ref()
                    .map(proto_value_to_string)
                    .unwrap_or_default();
                validate_loki_line_size(&line, &labels, limits)?;
                records.push(WalLogRecord {
                    tenant: tenant.to_owned(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line,
                    structured_metadata: proto_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}
