use super::{
    DistributorError, Limits, OtlpLine, OtlpResourceGroups, OtlpScope, OtlpScopeGroups,
    ProtoExportLogsServiceRequest, TenantId, WalLogRecord, otlp_resource_labels,
    otlp_scope_context, proto_log_record_structured_metadata, proto_timestamp_ns,
    proto_value_to_string,
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
            let groups = otlp_resource_labels(
                resource_logs
                    .resource
                    .as_ref()
                    .map(|resource| resource.attributes.as_slice())
                    .unwrap_or_default(),
                limits,
            )?;
            Ok(OtlpResourceGroups {
                labels: groups.labels,
                structured_metadata: groups.structured_metadata,
                scopes: resource_logs.scope_logs,
            })
        },
        |scope_logs, resource_labels, resource_metadata, limits| {
            let groups = otlp_scope_context(
                resource_labels,
                resource_metadata,
                &OtlpScope::from_proto(scope_logs.scope.as_ref()),
                limits,
            )?;
            Ok(OtlpScopeGroups {
                labels: groups.labels,
                inherited_metadata: groups.structured_metadata,
                records: scope_logs.log_records,
            })
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
                    .map(proto_value_to_string)
                    .unwrap_or_default(),
                attributes: &log_record.attributes,
            }
            .into_wal_record(|| proto_log_record_structured_metadata(&log_record), limits)
        },
    )
}
