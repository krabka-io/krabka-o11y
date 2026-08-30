use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};

use super::prelude::{
    AclOperation, AllowAllIngestLimiter, Arc, AtomicBool, BTreeMap, ByteSize, CONTENT_ENCODING,
    CONTENT_TYPE, DistributorState, HeaderMap, InMemoryWalSink, IngestQuotaBucket, KafkaWalHeader,
    Labels, NonZeroUsize, Offset, PartitionIndex, PatternType, PermissionType, ResourceType,
    ServiceMetrics, Time, WalLogRecord, acl_entry, acl_matches_tenant_wal_read,
    acl_matches_tenant_wal_write, async_trait, bytes, bytes_per_sec, check,
    check_tenant_wal_read_acl, check_tenant_wal_write_acl, decode_loki_http_body, encode_varint,
    has_native_kafka_log_headers, hot_tail_bucket_key, ingest_quota_bytes,
    is_loki_json_content_type, matches_acl_topic_pattern, measured_size, millis, minutes, secs,
    validate_ingest_body_limit,
};

// === split-modules: generated submodules ===
mod a_hot_tail_buffer_range_query_loses_no_record_to_its_buckets;
mod accumulating_a_wal_batch_stops_when_empty_or_full;
mod acl_helpers_require_topic_operation_principal_and_pattern;
mod hot_tail_bucket_key_uses_euclidean_minutes;
mod ingest_quota_bucket_and_byte_accounting_are_precise;
mod loki_content_type;
mod loki_content_type_and_body_decoding_accept_only_expected_forms;
mod native_header_detection_requires_native_log_shape;
mod only_an_object_store_compactor_error_is_classified_as_one;
mod varint_encoding_and_ingest_limits_pin_boundaries;

pub (crate) use a_hot_tail_buffer_range_query_loses_no_record_to_its_buckets::a_hot_tail_buffer_range_query_loses_no_record_to_its_buckets;
pub (crate) use accumulating_a_wal_batch_stops_when_empty_or_full::accumulating_a_wal_batch_stops_when_empty_or_full;
pub (crate) use acl_helpers_require_topic_operation_principal_and_pattern::acl_helpers_require_topic_operation_principal_and_pattern;
pub (crate) use hot_tail_bucket_key_uses_euclidean_minutes::hot_tail_bucket_key_uses_euclidean_minutes;
pub (crate) use ingest_quota_bucket_and_byte_accounting_are_precise::ingest_quota_bucket_and_byte_accounting_are_precise;
pub (crate) use loki_content_type::loki_content_type;
pub (crate) use loki_content_type_and_body_decoding_accept_only_expected_forms::loki_content_type_and_body_decoding_accept_only_expected_forms;
pub (crate) use native_header_detection_requires_native_log_shape::native_header_detection_requires_native_log_shape;
pub (crate) use only_an_object_store_compactor_error_is_classified_as_one::only_an_object_store_compactor_error_is_classified_as_one;
pub (crate) use varint_encoding_and_ingest_limits_pin_boundaries::varint_encoding_and_ingest_limits_pin_boundaries;
