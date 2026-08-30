use axum::response::IntoResponse;

use crate::{
    BTreeSet, Bytes, HeaderMap, Instant, Path, QuerierState, QueryKind, RawQuery, Response, State,
    StatusCode, Value, execute_detected_field_values_query, execute_detected_fields_query,
    execute_detected_labels_query, execute_format_query, execute_label_names_query,
    execute_patterns_query, handle_api_prom_query, handle_api_prom_query_range, handle_query, json,
    json_response, loki_success, parse_series_params, post_query_params,
    post_query_params_body_first,
};

// === split-modules: generated submodules ===
mod api_prom_query;
mod api_prom_query_post;
mod api_prom_query_range;
mod api_prom_query_range_post;
mod build_info;
mod detected_field_stats;
mod detected_field_type;
mod detected_field_values;
mod detected_field_values_post;
mod detected_fields;
mod detected_fields_params;
mod detected_fields_post;
mod detected_labels;
mod detected_labels_params;
mod detected_labels_post;
mod format_query;
mod format_query_post;
mod label_names;
mod label_names_post;
mod patterns;
mod patterns_params;
mod patterns_post;
mod query;
mod query_params;
mod query_post;
mod query_range;
mod query_range_post;
mod series_params;
mod status_metrics;
mod volume_aggregate_by;
mod volume_kind;
mod volume_params;

pub(crate) use api_prom_query::api_prom_query;
pub(crate) use api_prom_query_post::api_prom_query_post;
pub(crate) use api_prom_query_range::api_prom_query_range;
pub(crate) use api_prom_query_range_post::api_prom_query_range_post;
pub(crate) use build_info::build_info;
pub(crate) use detected_field_stats::DetectedFieldStats;
pub(crate) use detected_field_type::DetectedFieldType;
pub(crate) use detected_field_values::detected_field_values;
pub(crate) use detected_field_values_post::detected_field_values_post;
pub(crate) use detected_fields::detected_fields;
pub(crate) use detected_fields_params::DetectedFieldsParams;
pub(crate) use detected_fields_post::detected_fields_post;
pub(crate) use detected_labels::detected_labels;
pub(crate) use detected_labels_params::DetectedLabelsParams;
pub(crate) use detected_labels_post::detected_labels_post;
pub(crate) use format_query::format_query;
pub(crate) use format_query_post::format_query_post;
pub(crate) use label_names::label_names;
pub(crate) use label_names_post::label_names_post;
pub(crate) use patterns::patterns;
pub(crate) use patterns_params::PatternsParams;
pub(crate) use patterns_post::patterns_post;
pub(crate) use query::query;
pub(crate) use query_params::QueryParams;
pub(crate) use query_post::query_post;
pub(crate) use query_range::query_range;
pub(crate) use query_range_post::query_range_post;
pub(crate) use series_params::SeriesParams;
pub(crate) use status_metrics::status_metrics;
pub(crate) use volume_aggregate_by::VolumeAggregateBy;
pub(crate) use volume_kind::VolumeKind;
pub(crate) use volume_params::VolumeParams;
