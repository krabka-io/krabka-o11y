use axum::http::Extensions;
use clap::Parser as _;
use prost::Message as _;
use tower::ServiceExt as _;

use super::prelude::{
    Arc, BTreeMap, BlockIndex, BrokerAccessPolicy, CONTENT_ENCODING, CONTENT_TYPE,
    CancellationToken, ClientResourcePolicy, DeferredQueryAuthorizerConnect, Duration, HeaderMap,
    LabelIndex, Limits, Mutex, ObjectStore, Principal, ProtoAnyValue,
    ProtoExportLogsServiceRequest, ProtoKeyValue, ProtoLogRecord, QueryAuthorizationError, Role,
    RoleReadiness, ServiceConfig, ServiceDependencies, StatusCode, TenantId,
    UnavailableQueryAuthorizer, Url, Value, WalLogRecord, build_compactor_configured_object_store,
    build_service_router_with_shutdown, check, json, normalize_otlp_http_logs, proto_any_value,
    sleep, write_log_index_manifest,
};
use crate::{
    LogQueryAuthorizer as _,
    security_context::{MissingPrincipal, RequestSecurity, ServiceAudit},
};

mod a_request_through_no_listener_fails_closed_when_authentication_is_required;
mod a_role_with_a_broker_fails_closed_on_rules_and_deletes_until_its_authorizer_connects;
mod brute_force_in_range;
mod compactor_configured_object_store_builds_when_not_injected;
mod hot_tail_test_record;
mod normalize_otlp_http_logs_decodes_gzip_identically_to_identity;
mod recording_object_store;
mod service_readiness_requires_wal_and_authorization;
mod sorting_a_loki_vector_result_orders_only_a_vector;
mod unavailable_query_authorizer_fails_closed;

pub(crate) use brute_force_in_range::brute_force_in_range;
pub(crate) use hot_tail_test_record::hot_tail_test_record;
pub(crate) use recording_object_store::RecordingObjectStore;
