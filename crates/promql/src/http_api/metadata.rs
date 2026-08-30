use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};
use url::form_urlencoded;

use super::{
    ApiError, PrometheusApiState, apply_limit, parse_limit_parameter, success_data_response,
    tenant_from_headers,
};
use crate::{MetricStore, store::MetadataRecord};

// === split-modules: generated submodules ===
mod metadata;
mod metadata_json;
mod metadata_params;
mod parse_metadata_params;
mod target_metadata;
mod target_metadata_json;

pub(super) use metadata::metadata;
use metadata_json::metadata_json;
use metadata_params::MetadataParams;
use parse_metadata_params::parse_metadata_params;
pub(super) use target_metadata::target_metadata;
use target_metadata_json::target_metadata_json;
