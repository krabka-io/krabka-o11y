use super::{
    DistributorError, Labels, Limits, OtlpAttributeAction, OtlpLogsRequest, TenantId, WalLogRecord,
    discover_service_name_label, is_default_otlp_resource_label, otlp_attributes_to_labels,
    otlp_log_record_structured_metadata, otlp_timestamp_ns, otlp_value_to_string,
    truncate_loki_line, validate_loki_label_limits, validate_loki_line_size,
    validate_loki_timestamp_window, validate_structured_metadata_limits,
};

pub(crate) fn normalize_otlp_logs(
    tenant: &TenantId,
    payload: OtlpLogsRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_attribute_values = resource_logs
            .resource
            .as_ref()
            .and_then(|resource| resource.attributes.as_deref());
        let action = |name: &str| {
            limits
                .otlp_config
                .resource_attributes
                .attributes_config
                .iter()
                .find(|rule| rule.matches(name))
                .map_or_else(
                    || {
                        if !limits.otlp_config.resource_attributes.ignore_defaults
                            && is_default_otlp_resource_label(name)
                        {
                            OtlpAttributeAction::IndexLabel
                        } else {
                            OtlpAttributeAction::StructuredMetadata
                        }
                    },
                    |rule| rule.action,
                )
        };
        let mut resource_labels = Labels::default();
        let mut resource_metadata = Labels::default();
        let mut resource_names = std::collections::BTreeSet::new();
        for attribute in resource_attribute_values.unwrap_or_default() {
            let values = otlp_attributes_to_labels(Some(std::slice::from_ref(attribute)))?;
            if values
                .keys()
                .any(|name| !resource_names.insert(name.clone()))
            {
                return Err(DistributorError::InvalidOtlpAttribute);
            }
            match action(&attribute.key) {
                OtlpAttributeAction::IndexLabel => resource_labels.extend(values),
                OtlpAttributeAction::StructuredMetadata => resource_metadata.extend(values),
                OtlpAttributeAction::Drop => {}
            }
        }

        for scope_logs in resource_logs.scope_logs {
            let labels = resource_labels.clone();
            let mut inherited_metadata = resource_metadata.clone();
            for attribute in scope_logs
                .scope
                .as_ref()
                .and_then(|scope| scope.attributes.as_deref())
                .unwrap_or_default()
            {
                let action = limits
                    .otlp_config
                    .scope_attributes
                    .iter()
                    .find(|rule| rule.matches(&attribute.key))
                    .map(|rule| rule.action)
                    .unwrap_or_default();
                if action != OtlpAttributeAction::Drop {
                    inherited_metadata.extend(otlp_attributes_to_labels(Some(
                        std::slice::from_ref(attribute),
                    ))?);
                }
            }
            if let Some(scope) = &scope_logs.scope {
                if !scope.name.is_empty() {
                    inherited_metadata.insert("scope_name".to_string(), scope.name.clone());
                }
                if !scope.version.is_empty() {
                    inherited_metadata.insert("scope_version".to_string(), scope.version.clone());
                }
            }
            let mut labels = labels;
            discover_service_name_label(&mut labels);
            if labels.is_empty() {
                return Err(DistributorError::EmptyStreamLabels);
            }
            // The OTLP paths do not check the label *syntax*: the attribute
            // names come through `otlp_attributes_to_labels`, which already
            // shapes them. The caps are a different question and apply here.
            validate_loki_label_limits(&labels, limits)?;

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
                validate_loki_timestamp_window(timestamp_ns, &labels, limits)?;
                let mut line = log_record
                    .body
                    .as_ref()
                    .map(otlp_value_to_string)
                    .unwrap_or_default();
                truncate_loki_line(&mut line, limits);
                validate_loki_line_size(&line, &labels, limits)?;
                let mut structured_metadata = inherited_metadata.clone();
                structured_metadata.extend(otlp_log_record_structured_metadata(&log_record)?);
                for attribute in log_record.attributes.as_deref().unwrap_or_default() {
                    if limits
                        .otlp_config
                        .log_attributes
                        .iter()
                        .find(|rule| rule.matches(&attribute.key))
                        .map(|rule| rule.action)
                        == Some(OtlpAttributeAction::Drop)
                    {
                        for name in
                            otlp_attributes_to_labels(Some(std::slice::from_ref(attribute)))?.keys()
                        {
                            structured_metadata.remove(name);
                        }
                    }
                }
                validate_structured_metadata_limits(&structured_metadata, &labels, limits)?;
                records.push(WalLogRecord {
                    tenant: tenant.to_owned(),
                    labels: labels.clone(),
                    timestamp_ns,
                    line,
                    structured_metadata,
                    position: None,
                });
            }
        }
    }

    Ok(records)
}
