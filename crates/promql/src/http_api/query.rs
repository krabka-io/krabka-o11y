use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

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
    RequestAuth, StdDurationExt, TenantId, acquire_query_permit, apply_result_limit,
    authorized_tenant_from_headers, check_range_resolution, duration_param,
    enforce_query_range_limit, exemplar_key, exemplars_json, has_erasure_requests,
    optional_timestamp_ms, parse_limit_parameter, query_timeout, record_query_response,
    required_form_param, selector_matchers, success_data_response, success_response,
    success_response_with_stats, timestamp_ms, validate_timestamp_range,
};

#[allow(clippy::result_large_err)]
fn authorized_query_tenant(
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<TenantId, Response> {
    authorized_tenant_from_headers(headers, principal).map_err(IntoResponse::into_response)
}

pub(super) struct QueryDispatchRequest<'a, S: MetricStore, P> {
    pub state: &'a Arc<PrometheusApiState<S>>,
    pub headers: &'a HeaderMap,
    pub principal: &'a Principal,
    pub params: P,
    pub timing: QueryRequestTiming,
}

fn finish_query_evaluation<S: MetricStore>(
    state: &PrometheusApiState<S>,
    query_kind: &'static str,
    mut evaluated: EvaluatedQuery,
    eval_started: std::time::Instant,
    limit: Option<usize>,
    timing: QueryRequestTiming,
) -> Response {
    let evaluation = eval_started.elapsed();
    state.record_eval(
        query_kind,
        krabka_observability::service_metrics::RequestOutcome::from_result(&evaluated.outcome),
        evaluation.as_time(),
    );
    evaluated.evaluation = evaluation;
    evaluated_query_response(evaluated, limit, timing)
}
use crate::{
    AnnotatedQueryResult, MetricStore,
    engine::{PerStepSampleStats, collect_query_sample_stats, query_stats_step},
    planner::StepGrid,
    query_frontend::{FrontendRangeRequest, execute_range_query_frontend},
};

mod evaluated_query_response;
mod instant_query_params_from_form;
mod query_dispatch;
mod query_exemplars_inner;
mod query_range_dispatch;
mod query_params;
mod range_query_params_from_form;
mod run_timed_query;

pub(super) struct QueryRangeFormFields {
    pub(super) query: String,
    pub(super) start: String,
    pub(super) end: String,
}

pub(super) fn query_range_form_fields(
    body: &[u8],
    mut handle_extra: impl FnMut(&str, &str) -> Result<(), ApiError>,
) -> Result<QueryRangeFormFields, ApiError> {
    let mut query = None;
    let mut start = None;
    let mut end = None;
    for pair in form_urlencoded::parse(body) {
        let name = pair.0;
        let value = pair.1;
        match name.as_ref() {
            "query" => query = Some(value.into_owned()),
            "start" => start = Some(value.into_owned()),
            "end" => end = Some(value.into_owned()),
            _ => handle_extra(name.as_ref(), value.as_ref())?,
        }
    }
    Ok(QueryRangeFormFields {
        query: required_form_param(query, "query")?,
        start: required_form_param(start, "start")?,
        end: required_form_param(end, "end")?,
    })
}

use evaluated_query_response::{EvaluatedQuery, evaluated_query_response};
use instant_query_params_from_form::instant_query_params_from_form;
use query_dispatch::query_dispatch;
use query_exemplars_inner::query_exemplars_inner;
use query_range_dispatch::query_range_dispatch;

#[derive(Debug, Deserialize)]
pub(crate) struct ExemplarsQueryParams {
    pub(crate) query: String,
    pub(crate) start: String,
    pub(crate) end: String,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct QueryRequestTiming {
    pub(crate) started: Instant,
    pub(crate) queue: Duration,
}
use query_params::{InstantQueryParams, RangeQueryParams};
use range_query_params_from_form::range_query_params_from_form;
use run_timed_query::{TimedQuery, run_timed_query};

pub(crate) fn exemplars_query_params_from_form(
    body: &[u8],
) -> Result<ExemplarsQueryParams, ApiError> {
    let fields = query_range_form_fields(body, |_, _| Ok(()))?;
    Ok(ExemplarsQueryParams {
        query: fields.query,
        start: fields.start,
        end: fields.end,
    })
}

pub(crate) async fn query_exemplars<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match exemplars_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes())
        {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_exemplars_inner(state, headers, principal, params).await
}

pub(crate) async fn query_exemplars_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match exemplars_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_exemplars_inner(state, headers, principal, params).await
}

pub(crate) async fn query<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match instant_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes()) {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_inner(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
        params,
    )
    .await
}

pub(crate) async fn query_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: InstantQueryParams,
) -> Response {
    let timeout = params.timeout.clone();
    run_timed_query(
        state,
        TimedQuery {
            route: "query",
            timeout: timeout.as_deref(),
        },
        async |timing| {
            query_dispatch(QueryDispatchRequest {
                state,
                headers: auth.headers,
                principal: auth.principal,
                params,
                timing,
            })
            .await
        },
    )
    .await
}

pub(crate) async fn query_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match instant_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_inner(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
        params,
    )
    .await
}

pub(crate) async fn query_range<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params =
        match range_query_params_from_form(raw_query.as_deref().unwrap_or_default().as_bytes()) {
            Ok(params) => params,
            Err(error) => return error.into_response(),
        };
    query_range_inner(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
        params,
    )
    .await
}

pub(crate) async fn query_range_inner<S: MetricStore>(
    state: &Arc<PrometheusApiState<S>>,
    auth: RequestAuth<'_>,
    params: RangeQueryParams,
) -> Response {
    let timeout = params.timeout.clone();
    run_timed_query(
        state,
        TimedQuery {
            route: "query_range",
            timeout: timeout.as_deref(),
        },
        async |timing| {
            query_range_dispatch(QueryDispatchRequest {
                state,
                headers: auth.headers,
                principal: auth.principal,
                params,
                timing,
            })
            .await
        },
    )
    .await
}

pub(crate) async fn query_range_post<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let params = match range_query_params_from_form(&body) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    query_range_inner(
        &state,
        RequestAuth {
            headers: &headers,
            principal: &principal,
        },
        params,
    )
    .await
}
