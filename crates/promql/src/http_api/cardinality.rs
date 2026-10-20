use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json,
    extract::State,
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use krabka_metrics::{decode_float_samples, decode_native_histograms};

use super::{
    ApiError, CardinalityParams, ParsedForm, ParsedQuery, Principal, PrometheusApiState, Rejection,
    RequestAuth, RequestCaller, active_series_response, apply_limit,
    authorized_tenant_from_headers, cardinality_label_names_response,
    cardinality_label_values_response, enforce_selected_series_limit, labels_key,
    parse_cardinality_form, selector_matchers,
};
use crate::{MetricStore, PromqlLabels as Labels};

mod authorized_cardinality_series;
mod cardinality_active_native_histogram_metrics;

use authorized_cardinality_series::authorized_cardinality_series;
pub(super) use cardinality_active_native_histogram_metrics::{
    cardinality_active_native_histogram_metrics, cardinality_active_native_histogram_metrics_post,
};

pub(crate) async fn cardinality_active_series<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<CardinalityParams>,
) -> Response {
    cardinality_active_series_inner(state, caller.headers, caller.principal, params).await
}

pub(crate) async fn cardinality_label_names<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<CardinalityParams>,
) -> Response {
    cardinality_label_names_inner(&state, caller.auth(), params).await
}

pub(crate) async fn cardinality_label_names_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<CardinalityParams>,
) -> Response {
    cardinality_label_names_inner(&state, caller.auth(), params).await
}

pub(crate) async fn cardinality_label_values<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedQuery(params): ParsedQuery<CardinalityParams>,
) -> Response {
    cardinality_label_values_inner(&state, caller.auth(), params).await
}

pub(crate) async fn cardinality_label_values_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<CardinalityParams>,
) -> Response {
    cardinality_label_values_inner(&state, caller.auth(), params).await
}

pub(crate) async fn cardinality_active_series_inner<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    headers: HeaderMap,
    principal: Principal,
    params: CardinalityParams,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let series = cardinality_series(&state, tenant.as_str(), &params).await;
    match series {
        Ok(mut series) => {
            if let Err(error) = enforce_selected_series_limit(&state, &tenant, series.len()) {
                return error.into_response();
            }
            apply_limit(&mut series, params.limit);
            Json(active_series_response(series)).into_response()
        }
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn cardinality_active_series_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    ParsedForm(params): ParsedForm<CardinalityParams>,
) -> Response {
    let RequestCaller { principal, headers } = caller;
    cardinality_active_series_inner(state, headers, principal, params).await
}

pub(crate) async fn cardinality_label_names_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: CardinalityParams,
) -> Response {
    authorized_cardinality_series(state, auth, &params)
        .await
        .map(|selected| {
            Json(cardinality_label_names_response(
                &selected.series,
                params.limit,
            ))
        })
        .into_response()
}

pub(crate) async fn cardinality_label_values_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: CardinalityParams,
) -> Response {
    authorized_cardinality_series(state, auth, &params)
        .await
        .map(|selected| {
            Json(cardinality_label_values_response(
                &selected.series,
                &params.label_names,
                params.limit,
            ))
        })
        .into_response()
}

/// Resolves the series set a cardinality request operates on.
///
/// The result is the selector match when the request gives a `selector`. If the
/// request gives no `selector`, the result is every active series for the tenant.
pub(crate) async fn cardinality_series<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    params: &CardinalityParams,
) -> Result<Vec<Labels>, ApiError> {
    if params.selector.is_some() {
        cardinality_series_for_params(state, tenant, params).await
    } else {
        state
            .store
            .cardinality_active_series(tenant)
            .await
            .map_err(ApiError::from)
    }
}

pub(crate) async fn cardinality_series_for_params<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &str,
    params: &CardinalityParams,
) -> Result<Vec<Labels>, ApiError> {
    let selector = params.selector.as_deref().unwrap_or_default();
    let matcher_sets = selector_matchers(selector).map_err(ApiError::from)?;
    let mut by_key = BTreeMap::new();
    for matchers in matcher_sets {
        let series = state
            .store
            .series(tenant, &matchers, i64::MIN, i64::MAX)
            .await
            .map_err(ApiError::from)?;
        for labels in series {
            by_key.insert(labels_key(&labels), labels);
        }
    }
    Ok(by_key.into_values().collect())
}
