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

#[derive(Debug, Default, Deserialize)]
pub(crate) struct MetadataParams {
    pub(crate) metric: Option<String>,
    pub(crate) limit: Option<usize>,
    pub(crate) limit_per_metric: Option<usize>,
}

mod limited_metadata;
mod parse_metadata_params;

use limited_metadata::limited_metadata;
use parse_metadata_params::parse_metadata_params;

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

pub(crate) async fn metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    RawQuery(raw_query): RawQuery,
) -> Response {
    metadata_response_for_representation(state, caller, raw_query, MetadataRepresentation::Series)
        .await
}

pub(crate) fn metadata_json(
    metadata: Vec<MetadataRecord>,
    limit_per_metric: Option<usize>,
) -> BTreeMap<String, Vec<Value>> {
    let mut by_metric = BTreeMap::<String, Vec<Value>>::new();
    for record in metadata {
        let entries = by_metric.entry(record.metric_family_name).or_default();
        if limit_per_metric == Some(0) || limit_per_metric.is_none_or(|limit| entries.len() < limit)
        {
            entries.push(json!({
                "type": record.metric_type,
                "help": record.help,
                "unit": record.unit,
            }));
        }
    }
    by_metric
}

pub(crate) async fn target_metadata<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    caller: RequestCaller,
    RawQuery(raw_query): RawQuery,
) -> Response {
    metadata_response_for_representation(state, caller, raw_query, MetadataRepresentation::Targets)
        .await
}

pub(crate) fn target_metadata_json(metadata: Vec<MetadataRecord>) -> Vec<Value> {
    metadata
        .into_iter()
        .map(|record| {
            json!({
                "target": {},
                "metric": record.metric_family_name,
                "type": record.metric_type,
                "help": record.help,
                "unit": record.unit,
            })
        })
        .collect()
}
