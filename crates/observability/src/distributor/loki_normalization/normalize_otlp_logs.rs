use super::{
    DistributorError, Limits, OtlpLogsRequest, TenantId, WalLogRecord,
    otlp_log_record_structured_metadata, otlp_timestamp_ns, otlp_value_to_string,
};
use crate::distributor::otlp_normalization::{
    OtlpLine, OtlpScope, group_otlp_resource, group_otlp_scope, normalize_log_hierarchy,
};

pub(crate) fn normalize_otlp_logs(
    tenant: &TenantId,
    payload: OtlpLogsRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    normalize_log_hierarchy(
        tenant,
        payload.resource_logs,
        limits,
        |resource_logs, limits| {
            group_otlp_resource(
                resource_logs
                    .resource
                    .as_ref()
                    .and_then(|resource| resource.attributes.as_deref())
                    .unwrap_or_default(),
                resource_logs.scope_logs,
                limits,
            )
        },
        |scope_logs, resource_labels, resource_metadata, limits| {
            group_otlp_scope(
                resource_labels,
                resource_metadata,
                &OtlpScope::from_json(scope_logs.scope.as_ref()),
                scope_logs.log_records,
                limits,
            )
        },
        |log_record, labels, inherited, tenant, limits| {
            let zero_timestamp = log_record.time_unix_nano.is_null()
                || log_record.time_unix_nano.as_i64() == Some(0)
                || log_record.time_unix_nano.as_str() == Some("0");
            let timestamp = if zero_timestamp {
                log_record
                    .observed_time_unix_nano
                    .as_ref()
                    .unwrap_or(&log_record.time_unix_nano)
            } else {
                &log_record.time_unix_nano
            };
            OtlpLine {
                tenant,
                labels,
                inherited_metadata: inherited,
                timestamp_ns: otlp_timestamp_ns(timestamp)?,
                line: log_record
                    .body
                    .as_ref()
                    .map_or_else(Default::default, otlp_value_to_string),
                attributes: log_record.attributes.as_deref().unwrap_or_default(),
            }
            .into_wal_record(|| otlp_log_record_structured_metadata(&log_record), limits)
        },
    )
}
