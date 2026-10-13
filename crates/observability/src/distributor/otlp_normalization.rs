use krabka_units::convert::TimeExt;

use crate::{
    BTreeMap, DistributorError, LOKI_REJECT_OLD_SAMPLES_MAX_AGE, Labels, Limits,
    LokiProtoLabelPair, LokiProtoTimestamp, OffsetDateTime, OtlpAnyValue, OtlpAttributeAction,
    OtlpKeyValue, OtlpLogRecord, ProtoExportLogsServiceRequest, ProtoKeyValue, ProtoLogRecord,
    TenantId, Time, Value, WalLogRecord, current_unix_time_ns, encode_lower_hex,
    metadata_value_to_string, otlp_value_to_json, proto_any_value, proto_value_to_string,
    quote_logql_string, validate_loki_label_limits, validate_loki_line_size,
};

mod discover_detected_level_label;
mod discover_service_name_label;
mod insert_metadata_if_absent;
mod insert_proto_trace_context_metadata;
mod is_default_otlp_resource_label;
mod loki_missing_proto_timestamp_error;
mod loki_proto_label_pairs_to_labels;
mod loki_proto_timestamp_ns;
mod loki_stale_sample_label_set;
mod normalize_otlp_attribute_name;
mod normalize_otlp_proto_logs;
mod normalize_otlp_proto_logs_for_tenant;
mod otlp_attribute_labels;
mod otlp_attributes_to_labels;
mod otlp_log_record_structured_metadata;
mod otlp_log_steps;
mod otlp_severity_number_to_string;
mod otlp_timestamp_ns;
mod otlp_value_to_string;
mod proto_attributes_to_labels;
mod proto_log_record_structured_metadata;
mod proto_timestamp_ns;
mod rfc3339_seconds;
mod service_name_discovery_labels;
mod validate_ingest_timestamp_ns;
mod validate_loki_timestamp_window;
mod validate_loki_timestamp_window_at;

pub(crate) use discover_detected_level_label::discover_detected_level_label;
pub(crate) use discover_service_name_label::discover_service_name_label;
pub(crate) use insert_metadata_if_absent::insert_metadata_if_absent;
pub(crate) use insert_proto_trace_context_metadata::insert_proto_trace_context_metadata;
pub(crate) use is_default_otlp_resource_label::is_default_otlp_resource_label;
pub(crate) use loki_missing_proto_timestamp_error::loki_missing_proto_timestamp_error;
pub(crate) use loki_proto_label_pairs_to_labels::loki_proto_label_pairs_to_labels;
pub(crate) use loki_proto_timestamp_ns::loki_proto_timestamp_ns;
pub(crate) use loki_stale_sample_label_set::loki_stale_sample_label_set;
pub(crate) use normalize_otlp_attribute_name::normalize_otlp_attribute_name;
pub(crate) use normalize_otlp_proto_logs::normalize_otlp_proto_logs;
pub(crate) use normalize_otlp_proto_logs_for_tenant::normalize_otlp_proto_logs_for_tenant;
pub(crate) use otlp_attributes_to_labels::otlp_attributes_to_labels;
pub(crate) use otlp_log_record_structured_metadata::otlp_log_record_structured_metadata;
pub(crate) use otlp_log_steps::{OtlpLine, OtlpScope, otlp_resource_labels, otlp_scope_context};
pub(crate) use otlp_severity_number_to_string::otlp_severity_number_to_string;
pub(crate) use otlp_timestamp_ns::otlp_timestamp_ns;
pub(crate) use otlp_value_to_string::otlp_value_to_string;
pub(crate) use proto_attributes_to_labels::proto_attributes_to_labels;
pub(crate) use proto_log_record_structured_metadata::proto_log_record_structured_metadata;
pub(crate) use proto_timestamp_ns::proto_timestamp_ns;
pub(crate) use rfc3339_seconds::rfc3339_seconds;
pub(crate) use service_name_discovery_labels::SERVICE_NAME_DISCOVERY_LABELS;
pub(crate) use validate_ingest_timestamp_ns::validate_ingest_timestamp_ns;
pub(crate) use validate_loki_timestamp_window::validate_loki_timestamp_window;
pub(crate) use validate_loki_timestamp_window_at::validate_loki_timestamp_window_at;

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
