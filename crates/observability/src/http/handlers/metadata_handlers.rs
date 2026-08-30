use axum::response::IntoResponse;

use crate::{
    Bytes, HeaderMap, HttpQueryError, Instant, Path, QuerierState, QueryKind, QueryParams,
    RawQuery, Response, State, StatusCode, Value, VolumeKind, WebSocketUpgrade,
    add_loki_query_stats, authorized_tenants, execute_api_prom_label_names_query,
    execute_api_prom_series_query, execute_http_query_for_tenant, execute_index_stats_query,
    execute_index_volume_query, execute_label_values_query, execute_series_query, json,
    json_response, loki_instant_scalar_or_vector_response, loki_parquet_response,
    loki_range_vector_response, loki_success_value, merge_loki_query_response, parse_query_params,
    parse_series_params, post_query_params_body_first, prepare_http_tail,
    reject_signed_vector_function_literal, resolved_range_step, scalar_vector_expression_result,
    send_tail_stream, text_response, time_range, validate_loki_query_range_resolution,
    validate_loki_range_query_range_limit, wants_loki_parquet,
};

// === split-modules: generated submodules ===
mod api_prom_label_names;
mod api_prom_label_names_post;
mod api_prom_label_values;
mod api_prom_label_values_post;
mod api_prom_series;
mod api_prom_series_post;
mod api_prom_streams_only_response;
mod execute_http_multi_tenant_query;
mod execute_http_query;
mod handle_api_prom_query;
mod handle_api_prom_query_range;
mod handle_query;
mod index_stats;
mod index_stats_post;
mod index_volume;
mod index_volume_post;
mod index_volume_range;
mod index_volume_range_post;
mod label_values;
mod label_values_post;
mod series;
mod series_post;
mod tail;

pub(crate) use api_prom_label_names::api_prom_label_names;
pub(crate) use api_prom_label_names_post::api_prom_label_names_post;
pub(crate) use api_prom_label_values::api_prom_label_values;
pub(crate) use api_prom_label_values_post::api_prom_label_values_post;
pub(crate) use api_prom_series::api_prom_series;
pub(crate) use api_prom_series_post::api_prom_series_post;
pub(crate) use api_prom_streams_only_response::api_prom_streams_only_response;
pub(crate) use execute_http_multi_tenant_query::execute_http_multi_tenant_query;
pub(crate) use execute_http_query::execute_http_query;
pub(crate) use handle_api_prom_query::handle_api_prom_query;
pub(crate) use handle_api_prom_query_range::handle_api_prom_query_range;
pub(crate) use handle_query::handle_query;
pub(crate) use index_stats::index_stats;
pub(crate) use index_stats_post::index_stats_post;
pub(crate) use index_volume::index_volume;
pub(crate) use index_volume_post::index_volume_post;
pub(crate) use index_volume_range::index_volume_range;
pub(crate) use index_volume_range_post::index_volume_range_post;
pub(crate) use label_values::label_values;
pub(crate) use label_values_post::label_values_post;
pub(crate) use series::series;
pub(crate) use series_post::series_post;
pub(crate) use tail::tail;
