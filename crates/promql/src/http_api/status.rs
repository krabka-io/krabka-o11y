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
    ApiError, AuthorizedTenant, Extension, Principal, PrometheusApiState, apply_limit,
    authorized_tenant_from_headers, parse_limit_parameter, success_data_response,
};
use crate::{
    MetricStore,
    store::{NamedTsdbStat, TsdbBlock, TsdbStats},
};
mod runtime_info;
mod wal_replay_status;
pub(super) use runtime_info::runtime_info;
pub(super) use wal_replay_status::wal_replay_status;

pub(super) async fn status_flags<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
) -> Response {
    let retention = storage_retention(&state);
    success_data_response(json!({
        "log.level": state.status_log_level.as_str(),
        "query.lookback-delta": state.engine_opts.lookback_delta.human().to_string(),
        "query.max-concurrency": state.max_concurrent_queries.to_string(),
        "storage.tsdb.retention.time": retention,
    }))
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct TsdbStatusParams {
    pub(crate) limit: Option<usize>,
}

fn storage_retention<S: MetricStore>(state: &PrometheusApiState<S>) -> String {
    state.storage_retention.map_or_else(
        || "unknown (metric store does not expose retention)".to_string(),
        |retention| retention.human().to_string(),
    )
}

pub(crate) async fn alertmanagers() -> Response {
    success_data_response(json!({
        "activeAlertmanagers": [],
        "droppedAlertmanagers": [],
    }))
}

pub(crate) async fn scrape_pools() -> Response {
    success_data_response(json!([]))
}

pub(crate) async fn targets() -> Response {
    success_data_response(json!({
        "activeTargets": [],
        "droppedTargets": [],
        "droppedTargetCounts": {},
    }))
}

pub(crate) fn unix_time_string(time: SystemTime) -> String {
    time.duration_since(UNIX_EPOCH).map_or_else(
        |_| "0".to_string(),
        |duration| duration.as_secs().to_string(),
    )
}

pub(crate) fn named_tsdb_stats_json(
    mut stats: Vec<NamedTsdbStat>,
    limit: Option<usize>,
) -> Vec<Value> {
    apply_limit(&mut stats, limit);
    stats
        .into_iter()
        .map(|stat| {
            json!({
                "name": stat.name,
                "value": stat.value,
            })
        })
        .collect()
}

pub(crate) fn parse_tsdb_status_params(
    raw_query: Option<&str>,
) -> Result<TsdbStatusParams, ApiError> {
    let mut params = TsdbStatusParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for pair in form_urlencoded::parse(raw_query.as_bytes()) {
        let name = pair.0;
        let value = pair.1;
        if name == "limit" {
            params.limit = Some(parse_limit_parameter(&value)?);
        }
    }
    Ok(params)
}

pub(crate) async fn tsdb_blocks<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    AuthorizedTenant(tenant): AuthorizedTenant,
) -> Response {
    match state.store.tsdb_blocks(tenant.as_str()).await {
        Ok(blocks) => success_data_response(json!({
            "blocks": tsdb_blocks_json(blocks),
        })),
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(crate) async fn tsdb_status<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let params = match parse_tsdb_status_params(raw_query.as_deref()) {
        Ok(params) => params,
        Err(error) => return error.into_response(),
    };
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    match state.store.tsdb_stats(tenant.as_str()).await {
        Ok(tsdb) => success_data_response(tsdb_status_json(tsdb, params.limit)),
        Err(error) => ApiError::from(error).into_response(),
    }
}

pub(crate) fn tsdb_status_json(stats: TsdbStats, limit: Option<usize>) -> Value {
    json!({
        "headStats": {
            "numSeries": stats.head_stats.num_series,
            "chunkCount": stats.head_stats.num_chunks,
            "numSamples": stats.head_stats.num_samples,
            "minTime": stats.head_stats.min_time,
            "maxTime": stats.head_stats.max_time,
        },
        "seriesCountByMetricName": named_tsdb_stats_json(stats.series_count_by_metric_name, limit),
        "labelValueCountByLabelName": named_tsdb_stats_json(stats.label_value_count_by_label_name, limit),
        "memoryInBytesByLabelName": named_tsdb_stats_json(stats.memory_in_bytes_by_label_name, limit),
        "seriesCountByLabelValuePair": named_tsdb_stats_json(stats.series_count_by_label_value_pair, limit),
    })
}

pub(super) async fn build_info() -> Response {
    success_data_response(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "revision": build_value(option_env!("GIT_SHA").or(option_env!("GITHUB_SHA"))),
        "branch": build_value(option_env!("GIT_BRANCH").or(option_env!("GITHUB_REF_NAME"))),
        "buildUser": build_value(option_env!("BUILD_USER")),
        "buildDate": build_value(option_env!("BUILD_DATE")),
        "goVersion": "not applicable (Rust)",
    }))
}

fn build_value(value: Option<&'static str>) -> &'static str {
    value.filter(|value| !value.is_empty()).unwrap_or("unknown")
}

pub(super) async fn status_config<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
) -> Response {
    let retention = storage_retention(&state);
    success_data_response(json!({
        "yaml": format!(
            "krabka:\n  scrape_config: not applicable (Krabka does not scrape)\n  query_lookback_delta: {}\n  query_max_concurrency: {}\n  storage_retention: {retention}\n",
            state.engine_opts.lookback_delta.human(),
            state.max_concurrent_queries,
        ),
    }))
}

pub(crate) fn tsdb_blocks_json(mut blocks: Vec<TsdbBlock>) -> Vec<Value> {
    crate::store::sort_tsdb_blocks(&mut blocks);
    blocks
        .into_iter()
        .map(|block| {
            json!({
                "ulid": block.id,
                "minTime": block.min_time,
                "maxTime": block.max_time,
                "stats": {
                    "numSamples": block.num_samples,
                    "numSeries": block.num_series,
                    "numChunks": block.num_series,
                },
            })
        })
        .collect()
}
