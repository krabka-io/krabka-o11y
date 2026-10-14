use super::{
    DistributorError, Limits, OtlpLine, OtlpScope, ProtoExportLogsServiceRequest, TenantId,
    WalLogRecord, group_otlp_resource, group_otlp_scope, proto_log_record_structured_metadata,
    proto_timestamp_ns, proto_value_to_string,
};

pub(crate) fn normalize_otlp_proto_logs_for_tenant(
    tenant: &TenantId,
    payload: ProtoExportLogsServiceRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    super::normalize_log_hierarchy(
        tenant.as_str(),
        payload.resource_logs,
        limits,
        |resource_logs, limits| {
            group_otlp_resource(
                resource_logs
                    .resource
                    .as_ref()
                    .map_or_default(|resource| resource.attributes.as_slice()),
                resource_logs.scope_logs,
                limits,
            )
        },
        |scope_logs, resource_labels, resource_metadata, limits| {
            group_otlp_scope(
                resource_labels,
                resource_metadata,
                &OtlpScope::from_proto(scope_logs.scope.as_ref()),
                scope_logs.log_records,
                limits,
            )
        },
        |log_record, labels, inherited, tenant, limits| {
            OtlpLine {
                tenant,
                labels,
                inherited_metadata: inherited,
                timestamp_ns: proto_timestamp_ns(
                    log_record.time_unix_nano,
                    log_record.observed_time_unix_nano,
                )?,
                line: log_record
                    .body
                    .as_ref()
                    .map_or_default(proto_value_to_string),
                attributes: &log_record.attributes,
            }
            .into_wal_record(|| proto_log_record_structured_metadata(&log_record), limits)
        },
    )
}
