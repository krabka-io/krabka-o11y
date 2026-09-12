use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{RawQuery, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use krabka_units::prelude::*;
use serde::Deserialize;
use url::form_urlencoded;

use super::{
    ApiError, ERASURE_REQUEST_PREFIX, Extension, Principal, PrometheusApiState, QueryResponseStats,
    acquire_query_permit, apply_result_limit, authorized_tenant_from_headers,
    check_range_resolution, duration_param, enforce_query_range_limit, exemplar_key,
    exemplars_json, has_erasure_requests, optional_timestamp_ms, parse_limit_parameter,
    query_timeout, record_query_response, required_form_param, selector_matchers,
    success_data_response, success_response, success_response_with_stats, timestamp_ms,
    validate_timestamp_range,
};
use crate::{
    AnnotatedQueryResult, MetricStore,
    engine::{collect_query_sample_stats, query_stats_step},
    query_frontend::{FrontendRangeRequest, execute_range_query_frontend},
};

mod exemplars_query_params;
mod exemplars_query_params_from_form;
mod instant_query_params;
mod instant_query_params_from_form;
mod query_dispatch;
mod query_exemplars;
mod query_exemplars_inner;
mod query_exemplars_post;
mod query_fn;
mod query_inner;
mod query_post;
mod query_range;
mod query_range_dispatch;
mod query_range_inner;
mod query_range_post;
mod query_request_timing;
mod range_query_params;
mod range_query_params_from_form;

use exemplars_query_params::ExemplarsQueryParams;
use exemplars_query_params_from_form::exemplars_query_params_from_form;
use instant_query_params::InstantQueryParams;
use instant_query_params_from_form::instant_query_params_from_form;
use query_dispatch::query_dispatch;
pub(super) use query_exemplars::query_exemplars;
use query_exemplars_inner::query_exemplars_inner;
pub(super) use query_exemplars_post::query_exemplars_post;
pub(super) use query_fn::query;
use query_inner::query_inner;
pub(super) use query_post::query_post;
pub(super) use query_range::query_range;
use query_range_dispatch::query_range_dispatch;
use query_range_inner::query_range_inner;
pub(super) use query_range_post::query_range_post;
use query_request_timing::QueryRequestTiming;
use range_query_params::RangeQueryParams;
use range_query_params_from_form::range_query_params_from_form;
