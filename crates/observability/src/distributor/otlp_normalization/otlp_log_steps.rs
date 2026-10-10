use super::{
    DistributorError, Labels, Limits, OtlpAttributeAction, OtlpKeyValue, ProtoKeyValue,
    WalLogRecord, discover_detected_level_label, discover_service_name_label,
    is_default_otlp_resource_label, otlp_attributes_to_labels, proto_attributes_to_labels,
    validate_loki_label_limits, validate_loki_line_size, validate_loki_timestamp_window,
};
use crate::distributor::loki_normalization::{
    truncate_loki_line, validate_structured_metadata_limits,
};

/// An OTLP attribute in either encoding, OTLP/JSON or OTLP/protobuf.
pub(crate) trait OtlpAttribute {
    fn key(&self) -> &str;

    /// The labels the attribute flattens to.
    fn labels(&self) -> Result<Labels, DistributorError>;
}

impl OtlpAttribute for OtlpKeyValue {
    fn key(&self) -> &str {
        &self.key
    }

    fn labels(&self) -> Result<Labels, DistributorError> {
        otlp_attributes_to_labels(Some(std::slice::from_ref(self)))
    }
}

impl OtlpAttribute for ProtoKeyValue {
    fn key(&self) -> &str {
        &self.key
    }

    fn labels(&self) -> Result<Labels, DistributorError> {
        proto_attributes_to_labels(Some(std::slice::from_ref(self)))
    }
}

/// Splits resource attributes into stream labels and structured metadata, as
/// the tenant's OTLP resource-attribute rules direct.
///
/// Two attributes that flatten to one label name are an error.
pub(crate) fn otlp_resource_labels<A: OtlpAttribute>(
    attributes: &[A],
    limits: &Limits,
) -> Result<(Labels, Labels), DistributorError> {
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
    for attribute in attributes {
        let values = attribute.labels()?;
        if values
            .keys()
            .any(|name| !resource_names.insert(name.clone()))
        {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        match action(attribute.key()) {
            OtlpAttributeAction::IndexLabel => resource_labels.extend(values),
            OtlpAttributeAction::StructuredMetadata => resource_metadata.extend(values),
            OtlpAttributeAction::Drop => {}
        }
    }
    Ok((resource_labels, resource_metadata))
}

/// One OTLP instrumentation scope. A record with no scope has an empty one.
pub(crate) struct OtlpScope<'a, A> {
    pub(crate) attributes: &'a [A],
    pub(crate) name: &'a str,
    pub(crate) version: &'a str,
}

impl<'a> OtlpScope<'a, OtlpKeyValue> {
    /// The scope of an OTLP/JSON `scopeLogs` entry.
    pub(crate) fn from_json(scope: Option<&'a crate::distributor::router::OtlpScope>) -> Self {
        Self {
            attributes: scope
                .and_then(|scope| scope.attributes.as_deref())
                .unwrap_or_default(),
            name: scope.map_or("", |scope| scope.name.as_str()),
            version: scope.map_or("", |scope| scope.version.as_str()),
        }
    }
}

impl<'a> OtlpScope<'a, ProtoKeyValue> {
    /// The scope of an OTLP/protobuf `ScopeLogs` message.
    pub(crate) fn from_proto(
        scope: Option<&'a opentelemetry_proto::tonic::common::v1::InstrumentationScope>,
    ) -> Self {
        Self {
            attributes: scope
                .map(|scope| scope.attributes.as_slice())
                .unwrap_or_default(),
            name: scope.map_or("", |scope| scope.name.as_str()),
            version: scope.map_or("", |scope| scope.version.as_str()),
        }
    }
}

/// The structured metadata every record of `scope` inherits: the resource
/// metadata, the scope attributes the tenant's rules keep, and the scope's
/// name and version when they are not empty.
pub(crate) fn otlp_scope_metadata<A: OtlpAttribute>(
    resource_metadata: &Labels,
    scope: &OtlpScope<'_, A>,
    limits: &Limits,
) -> Result<Labels, DistributorError> {
    let mut inherited_metadata = resource_metadata.clone();
    for attribute in scope.attributes {
        let action = limits
            .otlp_config
            .scope_attributes
            .iter()
            .find(|rule| rule.matches(attribute.key()))
            .map(|rule| rule.action)
            .unwrap_or_default();
        if action != OtlpAttributeAction::Drop {
            inherited_metadata.extend(attribute.labels()?);
        }
    }
    if !scope.name.is_empty() {
        inherited_metadata.insert("scope_name".to_string(), scope.name.to_string());
    }
    if !scope.version.is_empty() {
        inherited_metadata.insert("scope_version".to_string(), scope.version.to_string());
    }
    Ok(inherited_metadata)
}

/// The stream labels of one scope's records: the resource labels with the
/// discovered `service_name`.
///
/// The OTLP paths do not check the label *syntax*: the attribute names come
/// through `otlp_attributes_to_labels` or `proto_attributes_to_labels`, which
/// already shape them. The caps are a different question and apply here.
pub(crate) fn otlp_stream_labels(
    resource_labels: &Labels,
    limits: &Limits,
) -> Result<Labels, DistributorError> {
    let mut labels = resource_labels.clone();
    discover_service_name_label(&mut labels);
    if labels.is_empty() {
        return Err(DistributorError::EmptyStreamLabels);
    }
    validate_loki_label_limits(&labels, limits)?;
    Ok(labels)
}

/// One OTLP log line, at `timestamp_ns`, validated against the tenant's limits
/// and turned into a WAL record.
///
/// The record's own structured metadata comes from `record_metadata`, which
/// runs only once the line has passed its checks. A record attribute that the
/// tenant's rules drop is removed from that metadata.
pub(crate) struct OtlpLine<'a, A> {
    pub tenant: &'a str,
    pub labels: &'a Labels,
    pub inherited_metadata: &'a Labels,
    pub timestamp_ns: i64,
    pub line: String,
    pub attributes: &'a [A],
}

impl<A: OtlpAttribute> OtlpLine<'_, A> {
    pub(crate) fn into_wal_record(
        self,
        record_metadata: impl FnOnce() -> Result<Labels, DistributorError>,
        limits: &Limits,
    ) -> Result<WalLogRecord, DistributorError> {
        let Self {
            tenant,
            labels,
            inherited_metadata,
            timestamp_ns,
            mut line,
            attributes,
        } = self;
        validate_loki_timestamp_window(timestamp_ns, labels, limits)?;
        truncate_loki_line(&mut line, limits);
        validate_loki_line_size(&line, labels, limits)?;
        let mut structured_metadata = inherited_metadata.clone();
        structured_metadata.extend(record_metadata()?);
        for attribute in attributes {
            if limits
                .otlp_config
                .log_attributes
                .iter()
                .find(|rule| rule.matches(attribute.key()))
                .map(|rule| rule.action)
                == Some(OtlpAttributeAction::Drop)
            {
                for name in attribute.labels()?.keys() {
                    structured_metadata.remove(name);
                }
            }
        }
        validate_structured_metadata_limits(&structured_metadata, labels, limits)?;
        discover_detected_level_label(labels, &mut structured_metadata, &line, limits);
        Ok(WalLogRecord {
            tenant: tenant.to_owned(),
            labels: labels.clone(),
            timestamp_ns,
            line,
            structured_metadata,
            position: None,
        })
    }
}
