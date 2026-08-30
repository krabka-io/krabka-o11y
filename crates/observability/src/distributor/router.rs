use axum::response::IntoResponse;
use krabka_units::convert::ByteSizeExt;
use tracing::Instrument;

use crate::{
    AllowAllIngestLimiter, Arc, AtomicBool, AtomicOrdering, ByteSize, Bytes, CONTENT_ENCODING,
    CONTENT_TYPE, Deserialize, HeaderMap, Instant, Labels, LogIngestLimiter, LogWalSink,
    LogsService, LogsServiceServer, ProtoExportLogsServiceRequest, ProtoExportLogsServiceResponse,
    Response, Router, ServiceMetrics, State, StatusCode, Time, Value,
    append_distributor_wal_records, build_info, distributor_error_to_grpc_status,
    flush_ingester_chunks, format_query, format_query_post, get, get_prepare_shutdown, grpc_tenant,
    log_level, log_level_post, measured_size, memberlist_status, normalize_loki_http_push,
    normalize_otlp_http_logs, normalize_otlp_proto_logs_for_tenant, otlp_http_error_response, post,
    ready, record_ingest_response, role_config, role_metrics, role_ring, role_services,
    set_prepare_shutdown, shutdown_ingester, unset_prepare_shutdown, validate_ingest_body_limit,
};

use crate::Extension;

// === split-modules: generated submodules ===
mod compactor_ops;
mod distributor_ops;
mod distributor_router;
mod distributor_router_with_sink;
mod distributor_state;
mod ingest_tenant;
mod loki_proto_entry;
mod loki_proto_label_pair;
mod loki_proto_push_request;
mod loki_proto_stream;
mod loki_proto_timestamp;
mod loki_push_request;
mod loki_push_stream;
mod loki_typed_push_request;
mod otlp_any_value;
mod otlp_array_value;
mod otlp_grpc_logs_service;
mod otlp_grpc_logs_service_with_limiter;
mod otlp_key_value;
mod otlp_key_value_list;
mod otlp_log_record;
mod otlp_logs_request;
mod otlp_resource;
mod otlp_resource_logs;
mod otlp_scope;
mod otlp_scope_logs;
mod push_logs;
mod push_otlp_logs;
mod querier_ops;
mod role_ops;
mod service_readiness;
mod with_role_ops_routes;

pub (crate) use compactor_ops::COMPACTOR_OPS;
pub (crate) use distributor_ops::DISTRIBUTOR_OPS;
pub use distributor_router::distributor_router;
pub (crate) use distributor_router_with_sink::distributor_router_with_sink;
pub use distributor_state::DistributorState;
pub (crate) use ingest_tenant::ingest_tenant;
pub (crate) use loki_proto_entry::LokiProtoEntry;
pub (crate) use loki_proto_label_pair::LokiProtoLabelPair;
pub (crate) use loki_proto_push_request::LokiProtoPushRequest;
pub (crate) use loki_proto_stream::LokiProtoStream;
pub (crate) use loki_proto_timestamp::LokiProtoTimestamp;
pub (crate) use loki_push_request::LokiPushRequest;
pub (crate) use loki_push_stream::LokiPushStream;
pub (crate) use loki_typed_push_request::LokiTypedPushRequest;
pub (crate) use otlp_any_value::OtlpAnyValue;
pub (crate) use otlp_array_value::OtlpArrayValue;
pub use otlp_grpc_logs_service::OtlpGrpcLogsService;
pub use otlp_grpc_logs_service::otlp_grpc_logs_service;
pub use otlp_grpc_logs_service_with_limiter::otlp_grpc_logs_service_with_limiter;
pub (crate) use otlp_key_value::OtlpKeyValue;
pub (crate) use otlp_key_value_list::OtlpKeyValueList;
pub (crate) use otlp_log_record::OtlpLogRecord;
pub (crate) use otlp_logs_request::OtlpLogsRequest;
pub (crate) use otlp_resource::OtlpResource;
pub (crate) use otlp_resource_logs::OtlpResourceLogs;
pub (crate) use otlp_scope::OtlpScope;
pub (crate) use otlp_scope_logs::OtlpScopeLogs;
pub (crate) use push_logs::push_logs;
pub (crate) use push_otlp_logs::push_otlp_logs;
pub (crate) use querier_ops::QUERIER_OPS;
pub (crate) use role_ops::RoleOps;
pub (crate) use service_readiness::ServiceReadiness;
pub (crate) use with_role_ops_routes::with_role_ops_routes;
