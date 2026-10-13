use super::{
    DistributorError, Limits, OtlpLogsRequest, TenantId, WalLogRecord,
    otlp_log_record_structured_metadata, otlp_timestamp_ns, otlp_value_to_string,
};
use crate::distributor::otlp_normalization::{
    OtlpLine, OtlpResourceGroups, OtlpScope, OtlpScopeGroups, normalize_log_hierarchy,
    otlp_resource_labels, otlp_scope_context,
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
            let groups = otlp_resource_labels(
                resource_logs
                    .resource
                    .as_ref()
                    .and_then(|resource| resource.attributes.as_deref())
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
                &OtlpScope::from_json(scope_logs.scope.as_ref()),
                limits,
            )?;
            Ok(OtlpScopeGroups {
                labels: groups.labels,
                inherited_metadata: groups.structured_metadata,
                records: scope_logs.log_records,
            })
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
                    .map(otlp_value_to_string)
                    .unwrap_or_default(),
                attributes: log_record.attributes.as_deref().unwrap_or_default(),
            }
            .into_wal_record(|| otlp_log_record_structured_metadata(&log_record), limits)
        },
    )
}
