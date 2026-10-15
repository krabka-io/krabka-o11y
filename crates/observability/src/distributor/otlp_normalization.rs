use krabka_units::convert::TimeExt;

use crate::{
    BTreeMap, DistributorError, LOKI_REJECT_OLD_SAMPLES_MAX_AGE, Labels, Limits,
    LokiProtoLabelPair, LokiProtoTimestamp, OffsetDateTime, OtlpAnyValue, OtlpAttributeAction,
    OtlpKeyValue, OtlpLogRecord, ProtoExportLogsServiceRequest, ProtoKeyValue, ProtoLogRecord,
    TenantId, Time, Value, WalLogRecord, current_unix_time_ns, encode_lower_hex,
    metadata_value_to_string, otlp_value_to_json, proto_any_value, proto_value_to_string,
    validate_loki_label_limits, validate_loki_line_size,
};

mod discover_detected_level_label;
mod otlp_log_steps;

pub(crate) use discover_detected_level_label::discover_detected_level_label;
pub(crate) use otlp_log_steps::{OtlpLine, OtlpScope, otlp_resource_labels, otlp_scope_context};

pub(crate) struct OtlpLabelGroups {
    pub(crate) labels: Labels,
    pub(crate) structured_metadata: Labels,
}

pub(crate) struct OtlpResourceGroups<S> {
    pub(crate) labels: Labels,
    pub(crate) structured_metadata: Labels,
    pub(crate) scopes: Vec<S>,
}

pub(crate) struct OtlpScopeGroups<L> {
    pub(crate) labels: Labels,
    pub(crate) inherited_metadata: Labels,
    pub(crate) records: Vec<L>,
}

pub(crate) fn group_otlp_resource<A, S>(
    attributes: &[A],
    scopes: Vec<S>,
    limits: &Limits,
) -> Result<OtlpResourceGroups<S>, DistributorError>
where
    A: otlp_log_steps::OtlpAttribute,
{
    let groups = otlp_resource_labels(attributes, limits)?;
    Ok(OtlpResourceGroups {
        labels: groups.labels,
        structured_metadata: groups.structured_metadata,
        scopes,
    })
}

pub(crate) fn group_otlp_scope<A, L>(
    resource_labels: &Labels,
    resource_metadata: &Labels,
    scope: &OtlpScope<'_, A>,
    records: Vec<L>,
    limits: &Limits,
) -> Result<OtlpScopeGroups<L>, DistributorError>
where
    A: otlp_log_steps::OtlpAttribute,
{
    let groups = otlp_scope_context(resource_labels, resource_metadata, scope, limits)?;
    Ok(OtlpScopeGroups {
        labels: groups.labels,
        inherited_metadata: groups.structured_metadata,
        records,
    })
}

pub(crate) fn normalize_log_hierarchy<R, S, L>(
    tenant: &str,
    resources: impl IntoIterator<Item = R>,
    limits: &Limits,
    mut resource: impl FnMut(R, &Limits) -> Result<OtlpResourceGroups<S>, DistributorError>,
    mut scope: impl FnMut(S, &Labels, &Labels, &Limits) -> Result<OtlpScopeGroups<L>, DistributorError>,
    mut record: impl FnMut(L, &Labels, &Labels, &str, &Limits) -> Result<WalLogRecord, DistributorError>,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let mut records = Vec::new();
    for resource_log in resources {
        let resource_groups = resource(resource_log, limits)?;
        for scope_log in resource_groups.scopes {
            let scope_groups = scope(
                scope_log,
                &resource_groups.labels,
                &resource_groups.structured_metadata,
                limits,
            )?;
            for log_record in scope_groups.records {
                records.push(record(
                    log_record,
                    &scope_groups.labels,
                    &scope_groups.inherited_metadata,
                    tenant,
                    limits,
                )?);
            }
        }
    }
    Ok(records)
}

pub(crate) fn insert_proto_trace_context_metadata(metadata: &mut Labels, name: &str, value: &[u8]) {
    if !value.is_empty() {
        metadata.insert(name.to_string(), encode_lower_hex(value));
    }
}

pub(crate) fn loki_proto_label_pairs_to_labels(labels: &[LokiProtoLabelPair]) -> Labels {
    let mut labels_by_name = Labels::new();
    for label in labels {
        labels_by_name.insert(label.name.clone(), label.value.clone());
    }
    labels_by_name
}

pub(crate) fn loki_stale_sample_label_set(labels: &Labels) -> String {
    crate::distributor::format_loki_label_set(labels, ", ")
}

pub(crate) fn normalize_otlp_proto_logs(
    tenant: &TenantId,
    payload: ProtoExportLogsServiceRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    normalize_otlp_proto_logs_for_tenant(tenant, payload, limits)
}

pub(crate) fn otlp_severity_number_to_string(value: &Value) -> Result<String, DistributorError> {
    match value {
        Value::Number(number) => Ok(number.to_string()),
        Value::String(string) => Ok(string.clone()),
        _ => Err(DistributorError::InvalidOtlpPayload),
    }
}

pub(crate) fn otlp_timestamp_ns(timestamp: &Value) -> Result<i64, DistributorError> {
    let timestamp_ns = match timestamp {
        Value::String(timestamp) => timestamp
            .parse()
            .map_err(|_| DistributorError::InvalidTimestamp),
        Value::Number(timestamp) => timestamp.as_i64().ok_or(DistributorError::InvalidTimestamp),
        _ => Err(DistributorError::InvalidTimestamp),
    }?;
    validate_ingest_timestamp_ns(timestamp_ns)
}

pub(crate) fn validate_ingest_timestamp_ns(timestamp_ns: i64) -> Result<i64, DistributorError> {
    if timestamp_ns < 0 {
        Err(DistributorError::InvalidTimestamp)
    } else {
        Ok(timestamp_ns)
    }
}

pub(crate) fn discover_service_name_label(labels: &mut Labels) {
    if labels.contains_key("service_name") {
        return;
    }

    let service_name = SERVICE_NAME_DISCOVERY_LABELS
        .iter()
        .filter_map(|name| labels.get(*name))
        .find(|value| !value.is_empty())
        .cloned()
        .unwrap_or_else(|| "unknown_service".to_string());
    labels.insert("service_name".to_string(), service_name);
}

pub(crate) fn insert_metadata_if_absent(
    metadata: &mut Labels,
    name: &str,
    value: Option<String>,
) -> Result<(), DistributorError> {
    let Some(value) = value else {
        return Ok(());
    };
    if metadata.insert(name.to_string(), value).is_some() {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    Ok(())
}

pub(crate) fn is_default_otlp_resource_label(name: &str) -> bool {
    matches!(
        name,
        "service.name"
            | "service.namespace"
            | "service.instance.id"
            | "deployment.environment"
            | "deployment.environment.name"
            | "cloud.region"
            | "cloud.availability_zone"
            | "k8s.cluster.name"
            | "k8s.namespace.name"
            | "k8s.pod.name"
            | "k8s.container.name"
            | "container.name"
            | "k8s.replicaset.name"
            | "k8s.deployment.name"
            | "k8s.statefulset.name"
            | "k8s.daemonset.name"
            | "k8s.cronjob.name"
            | "k8s.job.name"
    )
}

pub(crate) fn loki_missing_proto_timestamp_error(
    stream_labels: &Labels,
    max_age: Time,
) -> DistributorError {
    // With the window off there is still an oldest acceptable timestamp to
    // report, because the entry is rejected for having no timestamp at all.
    // `Loki`'s own default is what it would report.
    let max_age = if max_age > Time::ZERO {
        max_age
    } else {
        LOKI_REJECT_OLD_SAMPLES_MAX_AGE
    };
    let oldest_acceptable_timestamp_ns = current_unix_time_ns().saturating_sub(max_age.nanos_i64());
    DistributorError::TimestampTooOldString {
        stream: loki_stale_sample_label_set(stream_labels),
        timestamp: "0001-01-01T00:00:00Z",
        oldest_acceptable_timestamp_ns,
    }
}

pub(crate) fn loki_proto_timestamp_ns(
    timestamp: Option<&LokiProtoTimestamp>,
) -> Result<i64, DistributorError> {
    let timestamp = timestamp.ok_or(DistributorError::InvalidTimestamp)?;
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(DistributorError::InvalidTimestamp);
    }

    timestamp
        .seconds
        .checked_mul(1_000_000_000)
        .and_then(|seconds_ns| seconds_ns.checked_add(i64::from(timestamp.nanos)))
        .ok_or(DistributorError::InvalidTimestamp)
}

pub(crate) fn normalize_otlp_attribute_name(name: &str) -> String {
    let mut normalized = name
        .chars()
        .map(|ch| {
            if ch == '_' || ch.is_ascii_alphanumeric() {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        normalized.push('_');
    }
    if normalized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        normalized.insert(0, '_');
    }
    normalized
}

pub(crate) fn otlp_value_to_string(value: &OtlpAnyValue) -> String {
    match value {
        OtlpAnyValue::String(value) | OtlpAnyValue::Bytes(value) => value.clone(),
        OtlpAnyValue::Bool(value) => value.to_string(),
        OtlpAnyValue::Int(value) | OtlpAnyValue::Double(value) => metadata_value_to_string(value),
        OtlpAnyValue::Array(value) => serde_json::to_string(
            &value
                .values
                .as_deref()
                .unwrap_or_default()
                .iter()
                .map(otlp_value_to_json)
                .collect::<Vec<_>>(),
        )
        .expect("OTLP array values serialize to JSON"),
        OtlpAnyValue::Kvlist(value) => {
            serde_json::to_string(&value.values.as_deref().unwrap_or_default().iter().fold(
                BTreeMap::new(),
                |mut values, attribute| {
                    values.insert(attribute.key.clone(), otlp_value_to_json(&attribute.value));
                    values
                },
            ))
            .expect("OTLP key-value lists serialize to JSON")
        }
    }
}

pub(crate) fn proto_log_record_structured_metadata(
    log_record: &ProtoLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = proto_attributes_to_labels(Some(log_record.attributes.as_slice()))?;
    insert_metadata_if_absent(
        &mut metadata,
        "observed_timestamp",
        (log_record.observed_time_unix_nano != 0)
            .then(|| log_record.observed_time_unix_nano.to_string()),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        (log_record.severity_number != 0).then(|| log_record.severity_number.to_string()),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        (!log_record.severity_text.is_empty()).then(|| log_record.severity_text.clone()),
    )?;
    insert_proto_trace_context_metadata(&mut metadata, "trace_id", &log_record.trace_id);
    insert_proto_trace_context_metadata(&mut metadata, "span_id", &log_record.span_id);
    Ok(metadata)
}

pub(crate) fn proto_timestamp_ns(
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
) -> Result<i64, DistributorError> {
    let timestamp = if time_unix_nano == 0 {
        observed_time_unix_nano
    } else {
        time_unix_nano
    };
    i64::try_from(timestamp).map_err(|_| DistributorError::InvalidTimestamp)
}

pub(crate) fn rfc3339_seconds(timestamp_ns: i64) -> String {
    let seconds = timestamp_ns.div_euclid(1_000_000_000);
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(seconds) else {
        return seconds.to_string();
    };
    let date = timestamp.date();
    let time = timestamp.time();
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        date.year(),
        u8::from(date.month()),
        date.day(),
        time.hour(),
        time.minute(),
        time.second()
    )
}

pub(crate) fn validate_loki_timestamp_window(
    timestamp_ns: i64,
    stream_labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    validate_loki_timestamp_window_at(
        timestamp_ns,
        current_unix_time_ns(),
        stream_labels,
        limits.reject_old_samples_max_age,
        limits.creation_grace_period,
    )
}

pub(crate) fn normalize_otlp_proto_logs_for_tenant(
    tenant: &TenantId,
    payload: ProtoExportLogsServiceRequest,
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    normalize_log_hierarchy(
        tenant.as_str(),
        payload.resource_logs,
        limits,
        |resource_logs, limits| {
            group_otlp_resource(
                resource_logs
                    .resource
                    .as_ref()
                    .map_or_else(Default::default, |resource| resource.attributes.as_slice()),
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
                    .map_or_else(Default::default, proto_value_to_string),
                attributes: &log_record.attributes,
            }
            .into_wal_record(|| proto_log_record_structured_metadata(&log_record), limits)
        },
    )
}

pub(super) fn attributes_to_labels<A, K, C, V>(
    attributes: Option<&[A]>,
    key: K,
    children: C,
    label_value: V,
) -> Result<Labels, DistributorError>
where
    K: for<'a> Fn(&'a A) -> &'a str,
    C: for<'a> Fn(&'a A) -> Option<&'a [A]>,
    V: for<'a> Fn(&'a A) -> String,
{
    let mut labels = Labels::new();
    let accessors = AttributeAccessors {
        key: &key,
        children: &children,
        label_value: &label_value,
    };
    insert_attributes(&mut labels, "", attributes.unwrap_or_default(), &accessors)?;
    Ok(labels)
}

struct AttributeAccessors<'a, A> {
    key: &'a (dyn for<'b> Fn(&'b A) -> &'b str + 'a),
    children: &'a (dyn for<'b> Fn(&'b A) -> Option<&'b [A]> + 'a),
    label_value: &'a (dyn for<'b> Fn(&'b A) -> String + 'a),
}

fn insert_attributes<A>(
    labels: &mut Labels,
    prefix: &str,
    attributes: &[A],
    accessors: &AttributeAccessors<'_, A>,
) -> Result<(), DistributorError> {
    for attribute in attributes {
        if (accessors.key)(attribute).is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let key = if prefix.is_empty() {
            (accessors.key)(attribute).to_string()
        } else {
            format!("{prefix}.{}", (accessors.key)(attribute))
        };
        if let Some(children) = (accessors.children)(attribute) {
            insert_attributes(labels, &key, children, accessors)?;
        } else if labels
            .insert(
                normalize_otlp_attribute_name(&key),
                (accessors.label_value)(attribute),
            )
            .is_some()
        {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(())
}

pub(crate) fn otlp_attributes_to_labels(
    attributes: Option<&[OtlpKeyValue]>,
) -> Result<Labels, DistributorError> {
    attributes_to_labels(
        attributes,
        attribute_key,
        attribute_children,
        attribute_label_value,
    )
}

fn attribute_key(attribute: &OtlpKeyValue) -> &str {
    &attribute.key
}

fn attribute_children(attribute: &OtlpKeyValue) -> Option<&[OtlpKeyValue]> {
    match &attribute.value {
        OtlpAnyValue::Kvlist(values) => Some(values.values.as_deref().unwrap_or_default()),
        _ => None,
    }
}

fn attribute_label_value(attribute: &OtlpKeyValue) -> String {
    otlp_value_to_string(&attribute.value)
}

pub(crate) fn otlp_log_record_structured_metadata(
    log_record: &OtlpLogRecord,
) -> Result<Labels, DistributorError> {
    let mut metadata = otlp_attributes_to_labels(log_record.attributes.as_deref())?;
    insert_metadata_if_absent(
        &mut metadata,
        "observed_timestamp",
        log_record
            .observed_time_unix_nano
            .as_ref()
            .map(metadata_value_to_string),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_number",
        log_record
            .severity_number
            .as_ref()
            .map(otlp_severity_number_to_string)
            .transpose()?,
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "severity_text",
        log_record
            .severity_text
            .as_ref()
            .filter(|severity_text| !severity_text.is_empty())
            .cloned(),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "trace_id",
        log_record
            .trace_id
            .as_ref()
            .filter(|id| !id.is_empty())
            .cloned(),
    )?;
    insert_metadata_if_absent(
        &mut metadata,
        "span_id",
        log_record
            .span_id
            .as_ref()
            .filter(|id| !id.is_empty())
            .cloned(),
    )?;
    Ok(metadata)
}

pub(crate) fn proto_attributes_to_labels(
    attributes: Option<&[ProtoKeyValue]>,
) -> Result<Labels, DistributorError> {
    attributes_to_labels(
        attributes,
        proto_attribute_key,
        proto_attribute_children,
        proto_attribute_label_value,
    )
}

fn proto_attribute_key(attribute: &ProtoKeyValue) -> &str {
    &attribute.key
}

fn proto_attribute_children(attribute: &ProtoKeyValue) -> Option<&[ProtoKeyValue]> {
    match attribute
        .value
        .as_ref()
        .and_then(|value| value.value.as_ref())
    {
        Some(proto_any_value::Value::KvlistValue(values)) => Some(&values.values),
        _ => None,
    }
}

fn proto_attribute_label_value(attribute: &ProtoKeyValue) -> String {
    attribute
        .value
        .as_ref()
        .map_or_else(Default::default, proto_value_to_string)
}

pub(crate) const SERVICE_NAME_DISCOVERY_LABELS: &[&str] = &[
    "service",
    "app",
    "application",
    "name",
    "app_kubernetes_io_name",
    "container",
    "container_name",
    "component",
    "workload",
    "job",
];

/// The window check against a caller-supplied `now`.
///
/// Split out so the two bounds can be tested exactly at their edges. Both are
/// strict comparisons -- a timestamp precisely at the oldest or newest
/// acceptable value is accepted -- and against a wall clock that boundary is
/// unreachable: `now` advances between choosing the timestamp and reading it.
pub(crate) fn validate_loki_timestamp_window_at(
    timestamp_ns: i64,
    now_ns: i64,
    stream_labels: &Labels,
    max_age: Time,
    creation_grace_period: Time,
) -> Result<(), DistributorError> {
    if max_age > Time::ZERO {
        let oldest_acceptable_timestamp_ns = now_ns.saturating_sub(max_age.nanos_i64());
        if timestamp_ns < oldest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooOld {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
                oldest_acceptable_timestamp_ns,
            });
        }
    }
    if creation_grace_period > Time::ZERO {
        let newest_acceptable_timestamp_ns =
            now_ns.saturating_add(creation_grace_period.nanos_i64());
        if timestamp_ns > newest_acceptable_timestamp_ns {
            return Err(DistributorError::TimestampTooNew {
                stream: loki_stale_sample_label_set(stream_labels),
                timestamp_ns,
            });
        }
    }
    Ok(())
}
