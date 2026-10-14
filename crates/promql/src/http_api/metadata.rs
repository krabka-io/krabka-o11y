use std::{collections::BTreeMap, sync::Arc};

use axum::{
    extract::{RawQuery, State},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use serde_json::{Value, json};

use super::{
    ApiError, PrometheusApiState, Rejection, RequestAuth, RequestCaller, apply_limit,
    parse_limit_parameter, success_data_response,
};
use crate::{MetricStore, store::MetadataRecord};

mod limited_metadata;
mod metadata_fn;
mod metadata_json;
mod metadata_params;
mod parse_metadata_params;
mod target_metadata;
mod target_metadata_json;

use limited_metadata::limited_metadata;
pub(super) use metadata_fn::metadata;
use metadata_json::metadata_json;
use metadata_params::MetadataParams;
use parse_metadata_params::parse_metadata_params;
pub(super) use target_metadata::target_metadata;
use target_metadata_json::target_metadata_json;

async fn metadata_response<S, Render>(
    state: &PrometheusApiState<S>,
    auth: RequestAuth<'_>,
    raw_query: Option<&str>,
    render: Render,
) -> Response
where
    S: MetricStore,
    Render: FnOnce(MetadataParams, Vec<MetadataRecord>) -> Response,
{
    match limited_metadata(state, auth, raw_query).await {
        Ok(metadata) => render(metadata.params, metadata.records),
        Err(rejection) => rejection.into_response(),
    }
}

#[derive(Clone, Copy)]
pub(super) enum MetadataRepresentation {
    Series,
    Targets,
}

pub(super) async fn metadata_route<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    RawQuery(raw_query): RawQuery,
    representation: MetadataRepresentation,
) -> Response {
    metadata_response(
        &state,
        caller.auth(),
        raw_query.as_deref(),
        |params, metadata| match representation {
            MetadataRepresentation::Series => {
                success_data_response(metadata_json(metadata, params.limit_per_metric))
            }
            MetadataRepresentation::Targets => {
                success_data_response(target_metadata_json(metadata))
            }
        },
    )
    .await
}

pub(super) async fn metadata_response_for_representation<S: MetricStore>(
    state: Arc<PrometheusApiState<S>>,
    caller: RequestCaller,
    raw_query: Option<String>,
    representation: MetadataRepresentation,
) -> Response {
    metadata_route(State(state), caller, RawQuery(raw_query), representation).await
}
