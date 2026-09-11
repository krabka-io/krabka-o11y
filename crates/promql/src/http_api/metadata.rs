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
    ApiError, Extension, Principal, PrometheusApiState, apply_limit,
    authorized_tenant_from_headers, parse_limit_parameter, success_data_response,
};
use crate::{MetricStore, store::MetadataRecord};

mod metadata_fn;
mod metadata_json;
mod metadata_params;
mod parse_metadata_params;
mod target_metadata;
mod target_metadata_json;

pub(super) use metadata_fn::metadata;
use metadata_json::metadata_json;
use metadata_params::MetadataParams;
use parse_metadata_params::parse_metadata_params;
pub(super) use target_metadata::target_metadata;
use target_metadata_json::target_metadata_json;
