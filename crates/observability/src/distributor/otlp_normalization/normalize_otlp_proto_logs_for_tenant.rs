use super::{
    DistributorError, Limits, OtlpLine, OtlpScope, ProtoExportLogsServiceRequest, TenantId,
    WalLogRecord, otlp_resource_labels, otlp_scope_metadata, otlp_stream_labels,
    proto_log_record_structured_metadata, proto_timestamp_ns, proto_value_to_string,
};

pub(crate) fn normalize_otlp_proto_logs_for_tenant(
    tenant: &TenantId,
    payload: ProtoExportLogsServiceRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let (resource_labels, resource_metadata) = otlp_resource_labels(
            resource_logs
                .resource
                .as_ref()
                .map(|resource| resource.attributes.as_slice())
                .unwrap_or_default(),
            limits,
        )?;

        for scope_logs in resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            let inherited_metadata = otlp_scope_metadata(
                &resource_metadata,
                &OtlpScope {
                    attributes: scope
                        .map(|scope| scope.attributes.as_slice())
                        .unwrap_or_default(),
                    name: scope.map_or("", |scope| scope.name.as_str()),
                    version: scope.map_or("", |scope| scope.version.as_str()),
                },
                limits,
            )?;
            let labels = otlp_stream_labels(&resource_labels, limits)?;

            for log_record in scope_logs.log_records {
                let timestamp_ns = proto_timestamp_ns(
                    log_record.time_unix_nano,
                    log_record.observed_time_unix_nano,
                )?;
                let line = OtlpLine {
                    tenant,
                    labels: &labels,
                    inherited_metadata: &inherited_metadata,
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(proto_value_to_string)
                        .unwrap_or_default(),
                    attributes: &log_record.attributes,
                };
                records.push(line.into_wal_record(
                    || proto_log_record_structured_metadata(&log_record),
                    limits,
                )?);
            }
        }
    }

    Ok(records)
}
