use std::io::Read as _;

use axum::response::IntoResponse;
use krabka_units::convert::{ByteSizeExt, StdDurationExt, TimeExt};
use prost::Message;

use crate::{
    ByteSize, CONTENT_ENCODING, DeflateDecoder, DistributorError, DistributorState, GzDecoder,
    HeaderMap, Instant, LogIngestLimiter, LogWalSink, LokiProtoPushRequest, LokiPushRequest,
    LokiTypedPushRequest, ProtoExportLogsServiceRequest, Response, SnappyDecoder, StatusCode, Time,
    Value, WalLogRecord, WalSinkError, is_loki_json_content_type, is_protobuf_content_type,
    loki_json_timestamp_value_parse_error, normalize_loki_proto_push, normalize_loki_push,
    normalize_otlp_logs, normalize_otlp_proto_logs, quote_logql_string,
};

// === split-modules: generated submodules ===
mod append_distributor_wal_records;
mod append_wal_records;
mod check_ingest_quota;
mod decode_loki_http_body;
mod encode_otlp_status_message;
mod encode_varint;
mod loki_decode_error_context;
mod loki_json_push_labels_field_parse_error;
mod loki_json_push_payload_parse_error;
mod loki_json_push_stream_parse_error;
mod loki_json_push_streams_parse_error;
mod loki_json_push_value_parse_error;
mod loki_json_push_values_field_parse_error;
mod loki_structured_metadata_object_parse_error;
mod loki_structured_metadata_value_parse_error;
mod measured_size;
mod normalize_loki_http_push;
mod normalize_otlp_http_logs;
mod otlp_http_error_response;
mod previous_char_boundary;
mod record_ingest_response;
mod validate_ingest_body_limit;
mod validate_loki_json_push_stream_objects;
mod validate_loki_json_push_timestamp_types;
mod validate_loki_json_push_value_arrays;
mod validate_loki_json_structured_metadata_value_types;

pub(crate) use append_distributor_wal_records::append_distributor_wal_records;
pub(crate) use append_wal_records::append_wal_records;
pub(crate) use check_ingest_quota::check_ingest_quota;
pub(crate) use decode_loki_http_body::decode_loki_http_body;
pub(crate) use encode_otlp_status_message::encode_otlp_status_message;
pub(crate) use encode_varint::encode_varint;
pub(crate) use loki_decode_error_context::loki_decode_error_context;
pub(crate) use loki_json_push_labels_field_parse_error::loki_json_push_labels_field_parse_error;
pub(crate) use loki_json_push_payload_parse_error::loki_json_push_payload_parse_error;
pub(crate) use loki_json_push_stream_parse_error::loki_json_push_stream_parse_error;
pub(crate) use loki_json_push_streams_parse_error::loki_json_push_streams_parse_error;
pub(crate) use loki_json_push_value_parse_error::loki_json_push_value_parse_error;
pub(crate) use loki_json_push_values_field_parse_error::loki_json_push_values_field_parse_error;
pub(crate) use loki_structured_metadata_object_parse_error::loki_structured_metadata_object_parse_error;
pub(crate) use loki_structured_metadata_value_parse_error::loki_structured_metadata_value_parse_error;
pub(crate) use measured_size::measured_size;
pub(crate) use normalize_loki_http_push::normalize_loki_http_push;
pub(crate) use normalize_otlp_http_logs::normalize_otlp_http_logs;
pub(crate) use otlp_http_error_response::otlp_http_error_response;
pub(crate) use previous_char_boundary::previous_char_boundary;
pub(crate) use record_ingest_response::record_ingest_response;
pub(crate) use validate_ingest_body_limit::validate_ingest_body_limit;
pub(crate) use validate_loki_json_push_stream_objects::validate_loki_json_push_stream_objects;
pub(crate) use validate_loki_json_push_timestamp_types::validate_loki_json_push_timestamp_types;
pub(crate) use validate_loki_json_push_value_arrays::validate_loki_json_push_value_arrays;
pub(crate) use validate_loki_json_structured_metadata_value_types::validate_loki_json_structured_metadata_value_types;
