use super::{
    DistributorError, Labels, Limits, OtlpAttributeAction, ProtoExportLogsServiceRequest, TenantId,
    WalLogRecord, discover_service_name_label, is_default_otlp_resource_label,
    proto_attributes_to_labels, proto_log_record_structured_metadata, proto_timestamp_ns,
    proto_value_to_string, validate_loki_label_limits, validate_loki_line_size,
    validate_loki_timestamp_window,
};
use crate::{
    distributor::loki_normalization::validate_structured_metadata_limits, truncate_loki_line,
};

pub(crate) fn normalize_otlp_proto_logs_for_tenant(
    tenant: &TenantId,
    payload: ProtoExportLogsServiceRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let tenant = tenant.as_str();
    let mut records = Vec::new();

    for resource_logs in payload.resource_logs {
        let resource_attribute_values = resource_logs
            .resource
            .as_ref()
            .map(|resource| resource.attributes.as_slice());
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
            let values = proto_attributes_to_labels(Some(std::slice::from_ref(attribute)))?;
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
                .map(|scope| scope.attributes.as_slice())
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
                    inherited_metadata.extend(proto_attributes_to_labels(Some(
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
                let mut line = log_record
                    .body
                    .as_ref()
                    .map(proto_value_to_string)
                    .unwrap_or_default();
                truncate_loki_line(&mut line, limits);
                validate_loki_line_size(&line, &labels, limits)?;
                let mut structured_metadata = inherited_metadata.clone();
                structured_metadata.extend(proto_log_record_structured_metadata(&log_record)?);
                for attribute in &log_record.attributes {
                    if limits
                        .otlp_config
                        .log_attributes
                        .iter()
                        .find(|rule| rule.matches(&attribute.key))
                        .map(|rule| rule.action)
                        == Some(OtlpAttributeAction::Drop)
                    {
                        for name in
                            proto_attributes_to_labels(Some(std::slice::from_ref(attribute)))?
                                .keys()
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
