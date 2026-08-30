use super::*;

pub(crate) fn normalize_otlp_logs(
    headers: &HeaderMap,
    payload: OtlpLogsRequest,
    reject_old_samples_max_age: Option<Time>,
    creation_grace_period: Option<Time>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant(headers)?.to_string();
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

            for log_record in scope_logs.log_records {
                let timestamp_ns = otlp_timestamp_ns(&log_record.time_unix_nano)?;
                validate_loki_timestamp_window(
                    timestamp_ns,
                    &labels,
                    reject_old_samples_max_age,
                    creation_grace_period,
                )?;
                records.push(WalLogRecord {
                    tenant: tenant.clone(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line: log_record
                        .body
                        .as_ref()
                        .map(otlp_value_to_string)
                        .unwrap_or_default(),
                    structured_metadata: otlp_log_record_structured_metadata(&log_record)?,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}
