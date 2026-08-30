use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    extract::{RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use krabka_units::fmt::Human as _;
use serde::Deserialize;
use serde_json::{Value, json};
use url::form_urlencoded;

use super::{
    ApiError, PrometheusApiState, apply_limit, parse_limit_parameter, success_data_response,
    tenant_from_headers,
};
use crate::{
    MetricStore,
    store::{NamedTsdbStat, TsdbBlock, TsdbStats},
};

// === split-modules: generated submodules ===
mod alertmanagers;
mod build_info;
mod named_tsdb_stats_json;
mod parse_tsdb_status_params;
mod runtime_info;
mod scrape_pools;
mod status_config;
mod status_flags;
mod targets;
mod tsdb_blocks;
mod tsdb_blocks_json;
mod tsdb_status;
mod tsdb_status_json;
mod tsdb_status_params;
mod unix_time_string;
mod wal_replay_status;

pub (super) use alertmanagers::alertmanagers;
pub (super) use build_info::build_info;
use named_tsdb_stats_json::named_tsdb_stats_json;
use parse_tsdb_status_params::parse_tsdb_status_params;
pub (super) use runtime_info::runtime_info;
pub (super) use scrape_pools::scrape_pools;
pub (super) use status_config::status_config;
pub (super) use status_flags::status_flags;
pub (super) use targets::targets;
pub (super) use tsdb_blocks::tsdb_blocks;
use tsdb_blocks_json::tsdb_blocks_json;
pub (super) use tsdb_status::tsdb_status;
use tsdb_status_json::tsdb_status_json;
use tsdb_status_params::TsdbStatusParams;
use unix_time_string::unix_time_string;
pub (super) use wal_replay_status::wal_replay_status;
