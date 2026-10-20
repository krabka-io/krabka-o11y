use std::{collections::BTreeMap, sync::Arc};

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};

use super::{
    ApiError, DiscoveryParams, Extension, ParsedForm, ParsedQuery, Principal, PrometheusApiState,
    Rejection, RequestAuth, RequestCaller, apply_limit, discovery_matchers, discovery_window,
    enforce_query_range_limit, enforce_selected_series_limit, labels_json, labels_key,
    parse_discovery_form, record_query_response, success_data_response,
};
use crate::MetricStore;

/// A `/api/v1/label/{name}/values` request: the label it names and its
/// discovery parameters.
pub(crate) struct LabelValuesQuery {
    pub(crate) name: String,
    pub(crate) params: DiscoveryParams,
}

mod discovery_scope;
mod label_values_dispatch;
mod labels_dispatch;
mod series_dispatch;

use discovery_scope::{discovery_response, discovery_scope, limit_discovery_results};
use label_values_dispatch::label_values_dispatch;
use labels_dispatch::labels_dispatch;
use series_dispatch::series_dispatch;

pub(crate) async fn labels<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<DiscoveryParams>,
) -> Response {
    labels_inner(&state, caller.auth(), params).await
}

pub(crate) async fn labels_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<DiscoveryParams>,
) -> Response {
    labels_inner(&state, caller.auth(), params).await
}

pub(crate) async fn series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<DiscoveryParams>,
) -> Response {
    series_inner(&state, caller.auth(), params).await
}

pub(crate) async fn series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<DiscoveryParams>,
) -> Response {
    series_inner(&state, caller.auth(), params).await
}

pub(crate) async fn label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    Path(name): Path<String>,
    ParsedQuery(params): ParsedQuery<DiscoveryParams>,
) -> Response {
    label_values_inner(&state, caller.auth(), LabelValuesQuery { name, params }).await
}

pub(crate) async fn label_values_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    query: LabelValuesQuery,
) -> Response {
    let started = std::time::Instant::now();
    let response = label_values_dispatch(state, auth, query).await;
    record_query_response(state, "label_values", &response, started);
    response
}

pub(crate) async fn label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    let params = match parse_discovery_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    label_values_inner(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
        LabelValuesQuery { name, params },
    )
    .await
}

pub(crate) async fn labels_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = labels_dispatch(state, auth, params).await;
    record_query_response(state, "labels", &response, started);
    response
}

pub(crate) async fn series_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: DiscoveryParams,
) -> Response {
    let started = std::time::Instant::now();
    let response = series_dispatch(state, auth, params).await;
    record_query_response(state, "series", &response, started);
    response
}
