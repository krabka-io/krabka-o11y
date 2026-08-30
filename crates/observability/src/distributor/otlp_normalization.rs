use krabka_units::convert::TimeExt;

use crate::{
    BTreeMap, DistributorError, HeaderMap, LOKI_REJECT_OLD_SAMPLES_MAX_AGE, Labels,
    LokiProtoLabelPair, LokiProtoTimestamp, OffsetDateTime, OtlpAnyValue, OtlpKeyValue,
    OtlpLogRecord, ProtoExportLogsServiceRequest, ProtoKeyValue, ProtoLogRecord, Time, Value,
    WalLogRecord, current_unix_time_ns, hex_string, metadata_value_to_string, otlp_value_to_json,
    proto_value_to_string, quote_logql_string, tenant,
};

// === split-modules: generated submodules ===
mod contains_log_level_token;
mod detect_log_level;
mod discover_detected_level_label;
mod discover_service_name_label;
mod insert_metadata_if_absent;
mod insert_proto_trace_context_metadata;
mod is_log_level_word_byte;
mod loki_missing_proto_timestamp_error;
mod loki_proto_label_pairs_to_labels;
mod loki_proto_timestamp_ns;
mod loki_stale_sample_label_set;
mod normalize_otlp_attribute_name;
mod normalize_otlp_proto_logs;
mod normalize_otlp_proto_logs_for_tenant;
mod otlp_attributes_to_labels;
mod otlp_log_record_structured_metadata;
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

pub(crate) use contains_log_level_token::contains_log_level_token;
pub(crate) use detect_log_level::detect_log_level;
pub(crate) use discover_detected_level_label::discover_detected_level_label;
pub(crate) use discover_service_name_label::discover_service_name_label;
pub(crate) use insert_metadata_if_absent::insert_metadata_if_absent;
pub(crate) use insert_proto_trace_context_metadata::insert_proto_trace_context_metadata;
pub(crate) use is_log_level_word_byte::is_log_level_word_byte;
pub(crate) use loki_missing_proto_timestamp_error::loki_missing_proto_timestamp_error;
pub(crate) use loki_proto_label_pairs_to_labels::loki_proto_label_pairs_to_labels;
pub(crate) use loki_proto_timestamp_ns::loki_proto_timestamp_ns;
pub(crate) use loki_stale_sample_label_set::loki_stale_sample_label_set;
pub(crate) use normalize_otlp_attribute_name::normalize_otlp_attribute_name;
pub(crate) use normalize_otlp_proto_logs::normalize_otlp_proto_logs;
pub(crate) use normalize_otlp_proto_logs_for_tenant::normalize_otlp_proto_logs_for_tenant;
pub(crate) use otlp_attributes_to_labels::otlp_attributes_to_labels;
pub(crate) use otlp_log_record_structured_metadata::otlp_log_record_structured_metadata;
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
