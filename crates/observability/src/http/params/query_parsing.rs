use krabka_units::convert::TimeExt;

use crate::{
    DetectedFieldsParams, DetectedLabelsParams, HttpQueryError, LOKI_DEFAULT_QUERY_RANGE,
    LOKI_MAX_TAIL_DELAY, OffsetDateTime, PatternsParams, QueryParams, Rfc3339, VolumeAggregateBy,
    VolumeParams, current_unix_time_ns, decode_form_component, parse_decimal_seconds_timestamp,
    parse_usize_query_param, start_or_since,
};

// === split-modules: generated submodules ===
mod parse_detected_fields_params;
mod parse_detected_labels_params;
mod parse_loki_duration_query_param;
mod parse_loki_tail_delay_for_query_param;
mod parse_loki_timestamp_query_param;
mod parse_patterns_params;
mod parse_prometheus_duration;
mod parse_query_params;
mod parse_volume_params;
mod prometheus_duration_unit;
mod split_query_param_pairs;
mod validate_loki_tail_delay_for;

pub(crate) use parse_detected_fields_params::parse_detected_fields_params;
pub(crate) use parse_detected_labels_params::parse_detected_labels_params;
pub(crate) use parse_loki_duration_query_param::parse_loki_duration_query_param;
pub(crate) use parse_loki_tail_delay_for_query_param::parse_loki_tail_delay_for_query_param;
pub(crate) use parse_loki_timestamp_query_param::parse_loki_timestamp_query_param;
pub(crate) use parse_patterns_params::parse_patterns_params;
pub(crate) use parse_prometheus_duration::parse_prometheus_duration;
pub(crate) use parse_query_params::parse_query_params;
pub(crate) use parse_volume_params::parse_volume_params;
pub(crate) use prometheus_duration_unit::prometheus_duration_unit;
pub(crate) use split_query_param_pairs::split_query_param_pairs;
pub(crate) use validate_loki_tail_delay_for::validate_loki_tail_delay_for;
