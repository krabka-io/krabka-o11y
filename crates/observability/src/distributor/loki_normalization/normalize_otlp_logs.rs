use super::{
    DistributorError, Limits, OtlpLogsRequest, TenantId, WalLogRecord,
    otlp_log_record_structured_metadata, otlp_timestamp_ns, otlp_value_to_string,
};
use crate::distributor::otlp_normalization::{
    OtlpLine, OtlpScope, otlp_resource_labels, otlp_scope_metadata, otlp_stream_labels,
};

pub(crate) fn normalize_otlp_logs(
    tenant: &TenantId,
    payload: OtlpLogsRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let (resource_labels, resource_metadata) = otlp_resource_labels(
            resource_logs
                .resource
                .as_ref()
                .and_then(|resource| resource.attributes.as_deref())
                .unwrap_or_default(),
            limits,
        )?;

        for scope_logs in resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            let inherited_metadata = otlp_scope_metadata(
                &resource_metadata,
                &OtlpScope {
                    attributes: scope
                        .and_then(|scope| scope.attributes.as_deref())
                        .unwrap_or_default(),
                    name: scope.map_or("", |scope| scope.name.as_str()),
                    version: scope.map_or("", |scope| scope.version.as_str()),
                },
                limits,
            )?;
            let labels = otlp_stream_labels(&resource_labels, limits)?;

            for log_record in scope_logs.log_records {
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
                let timestamp_ns = otlp_timestamp_ns(timestamp)?;
                let line = OtlpLine {
                    tenant,
                    labels: &labels,
                    inherited_metadata: &inherited_metadata,
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(otlp_value_to_string)
                        .unwrap_or_default(),
                    attributes: log_record.attributes.as_deref().unwrap_or_default(),
                };
                records.push(line.into_wal_record(
                    || otlp_log_record_structured_metadata(&log_record),
                    limits,
                )?);
            }
        }
    }

    Ok(records)
}
