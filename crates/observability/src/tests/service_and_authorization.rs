use clap::Parser as _;
use prost::Message as _;

use super::prelude::{
    Arc, AtomicOrdering, BTreeMap, CONTENT_ENCODING, CONTENT_TYPE, Duration, HeaderMap, Mutex,
    ObjectStore, ProtoAnyValue, ProtoExportLogsServiceRequest, ProtoKeyValue, ProtoLogRecord,
    QueryAuthorizationError, ServiceConfig, ServiceReadiness, UnavailableQueryAuthorizer, Url,
    WalLogRecord, build_compactor_configured_object_store, check, ingest_tenant,
    normalize_otlp_http_logs, proto_any_value, sleep,
};
use crate::LogQueryAuthorizer as _;

mod brute_force_in_range;
mod compactor_configured_object_store_builds_when_not_injected;
mod hot_tail_test_record;
mod ingest_tenant_reads_header_or_falls_back;
mod normalize_otlp_http_logs_decodes_gzip_identically_to_identity;
mod recording_object_store;
mod service_readiness_requires_wal_and_authorization;
mod sorting_a_loki_vector_result_orders_only_a_vector;
mod unavailable_query_authorizer_fails_closed;

pub(crate) use brute_force_in_range::brute_force_in_range;
pub(crate) use hot_tail_test_record::hot_tail_test_record;
pub(crate) use recording_object_store::RecordingObjectStore;
