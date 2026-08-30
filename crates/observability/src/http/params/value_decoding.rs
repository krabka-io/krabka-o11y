use krabka_units::convert::TimeExt;

use crate::{
    HeaderMap, HttpQueryError, QuerierState, QueryParams, SystemTime, Time, TimeRange, UNIX_EPOCH,
    hours, secs,
};

// === split-modules: generated submodules ===
mod authorized_tenant;
mod authorized_tenants;
mod current_unix_time_ns;
mod decode_form_component;
mod grpc_tenant;
mod hex_value;
mod loki_default_query_range;
mod loki_default_tail_limit;
mod loki_direction;
mod loki_max_query_range_resolution_points;
mod loki_max_tail_delay;
mod loki_metadata_default_index_range;
mod loki_volume_max_query_range;
mod optional_start_end_range;
mod parse_decimal_seconds_timestamp;
mod parse_usize_query_param;
mod query_kind;
mod start_or_since;
mod tenant;
mod time_range;

pub (crate) use authorized_tenant::authorized_tenant;
pub (crate) use authorized_tenants::authorized_tenants;
pub (crate) use current_unix_time_ns::current_unix_time_ns;
pub (crate) use decode_form_component::decode_form_component;
pub (crate) use grpc_tenant::grpc_tenant;
pub (crate) use hex_value::hex_value;
pub (crate) use loki_default_query_range::LOKI_DEFAULT_QUERY_RANGE;
pub (crate) use loki_default_tail_limit::LOKI_DEFAULT_TAIL_LIMIT;
pub (crate) use loki_direction::LokiDirection;
pub (crate) use loki_direction::loki_direction;
pub (crate) use loki_max_query_range_resolution_points::LOKI_MAX_QUERY_RANGE_RESOLUTION_POINTS;
pub (crate) use loki_max_tail_delay::LOKI_MAX_TAIL_DELAY;
pub (crate) use loki_metadata_default_index_range::LOKI_METADATA_DEFAULT_INDEX_RANGE;
pub (crate) use loki_volume_max_query_range::LOKI_VOLUME_MAX_QUERY_RANGE;
pub (crate) use optional_start_end_range::optional_start_end_range;
pub (crate) use parse_decimal_seconds_timestamp::parse_decimal_seconds_timestamp;
pub (crate) use parse_usize_query_param::parse_usize_query_param;
pub (crate) use query_kind::QueryKind;
pub (crate) use start_or_since::start_or_since;
pub (crate) use tenant::tenant;
pub (crate) use time_range::time_range;
