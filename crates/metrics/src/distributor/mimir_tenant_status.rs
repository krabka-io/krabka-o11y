use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Extension, Json,
    extract::{Query, State},
    http::{HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use krabka_observability::server_security::{Principal, authorize_admin};
use krabka_units::convert::{FrequencyExt as _, TimeExt as _};
use num_traits::ToPrimitive as _;
use serde::Serialize;

use super::DistributorState;
use crate::{Limits, authorized_tenant_from_headers};

#[derive(Serialize)]
struct UserLimitsResponse {
    compactor_blocks_retention_period_seconds: i64,
    ingestion_rate: f64,
    ingestion_burst_size: u64,
    ingestion_burst_factor: f64,
    max_global_series_per_user: u64,
    max_global_series_per_metric: u64,
    max_global_exemplars_per_user: u64,
    max_fetched_chunks_per_query: u64,
    max_fetched_series_per_query: u64,
    max_fetched_chunk_bytes_per_query: u64,
    ruler_max_rules_per_rule_group: u64,
    ruler_max_rule_groups_per_tenant: u64,
    alertmanager_notification_rate_limit: f64,
    alertmanager_max_dispatcher_aggregation_groups: u64,
    alertmanager_max_templates_count: u64,
    alertmanager_max_alerts_count: u64,
}

impl From<&Limits> for UserLimitsResponse {
    fn from(limits: &Limits) -> Self {
        Self {
            compactor_blocks_retention_period_seconds: limits
                .compactor_blocks_retention_period
                .secs_f64()
                .round()
                .to_i64()
                .unwrap_or(i64::MAX),
            ingestion_rate: limits.ingestion_rate.per_sec_f64(),
            ingestion_burst_size: limits.ingestion_burst_size,
            ingestion_burst_factor: 0.0,
            max_global_series_per_user: limits.max_global_series_per_user,
            max_global_series_per_metric: 0,
            max_global_exemplars_per_user: 0,
            max_fetched_chunks_per_query: 0,
            max_fetched_series_per_query: limits.max_fetched_series_per_query,
            max_fetched_chunk_bytes_per_query: 0,
            ruler_max_rules_per_rule_group: 0,
            ruler_max_rule_groups_per_tenant: 0,
            alertmanager_notification_rate_limit: 0.0,
            alertmanager_max_dispatcher_aggregation_groups: 0,
            alertmanager_max_templates_count: 0,
            alertmanager_max_alerts_count: 0,
        }
    }
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct UserStats {
    ingestion_rate: f64,
    num_series: u64,
    #[serde(rename = "APIIngestionRate")]
    api_rate: f64,
    #[serde(rename = "RuleIngestionRate")]
    rule_rate: f64,
}

#[derive(Serialize)]
struct UserIdStats {
    #[serde(rename = "userID")]
    user_id: String,
    #[serde(flatten)]
    stats: UserStats,
}

#[derive(Serialize)]
struct RuntimeConfig<'a> {
    defaults: &'a Limits,
    overrides: BTreeMap<&'a str, &'a Limits>,
}

#[derive(Default, serde::Deserialize)]
pub(crate) struct RuntimeConfigQuery {
    mode: Option<String>,
}

pub(crate) async fn user_limits(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: axum::http::HeaderMap,
) -> Response {
    match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => {
            Json(UserLimitsResponse::from(state.limits_for_tenant(&tenant))).into_response()
        }
        Err(error) => error.into_response(),
    }
}

pub(crate) async fn user_stats(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
    headers: axum::http::HeaderMap,
) -> Response {
    let tenant = match authorized_tenant_from_headers(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let num_series = state
        .series_tracker
        .lock()
        .tenants
        .get(&tenant)
        .map_or(0, |tracked| {
            u64::try_from(tracked.series.len()).unwrap_or(u64::MAX)
        });
    Json(UserStats {
        ingestion_rate: 0.0,
        num_series,
        api_rate: 0.0,
        rule_rate: 0.0,
    })
    .into_response()
}

pub(crate) async fn all_user_stats(
    State(state): State<Arc<DistributorState>>,
    Extension(principal): Extension<Principal>,
) -> Response {
    if let Err(error) = authorize_admin(&principal) {
        return error.into_response();
    }
    let user_stats = state
        .series_tracker
        .lock()
        .tenants
        .iter()
        .map(|(tenant, tracked)| UserIdStats {
            user_id: tenant.as_str().to_owned(),
            stats: UserStats {
                ingestion_rate: 0.0,
                num_series: u64::try_from(tracked.series.len()).unwrap_or(u64::MAX),
                api_rate: 0.0,
                rule_rate: 0.0,
            },
        })
        .collect::<Vec<_>>();
    Json(user_stats).into_response()
}

pub(crate) async fn runtime_config(
    State(state): State<Arc<DistributorState>>,
    Query(query): Query<RuntimeConfigQuery>,
) -> Response {
    let overrides = state
        .overrides
        .per_tenant
        .iter()
        .map(|(tenant, limits)| (tenant.as_str(), limits))
        .collect();
    let output = RuntimeConfig {
        defaults: &state.overrides.defaults,
        overrides,
    };
    let output = if query.mode.as_deref() == Some("diff") && output.overrides.is_empty() {
        "{}\n".to_owned()
    } else {
        match serde_yaml::to_string(&output) {
            Ok(output) => output,
            Err(error) => {
                return (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response();
            }
        }
    };
    (
        [(CONTENT_TYPE, HeaderValue::from_static("application/yaml"))],
        output,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use axum::body::to_bytes;
    use bytes::Bytes;
    use krabka_observability::server_security::Principal;

    use super::*;
    use crate::{distributor::ProduceError, wal::WalRecord};

    struct NoopSink;

    #[async_trait::async_trait]
    impl super::super::WalSink for NoopSink {
        async fn append(&self, _: Bytes, _: WalRecord) -> Result<(), ProduceError> {
            Ok(())
        }
    }

    fn state() -> Arc<DistributorState> {
        Arc::new(DistributorState::new(Arc::new(NoopSink)))
    }

    fn tenant_headers() -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-scope-orgid", HeaderValue::from_static("tenant-a"));
        headers
    }

    async fn body(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn tenant_limits_and_live_series_stats_use_the_distributor_state() {
        let distributor = state();
        let limits = body(
            user_limits(
                State(Arc::clone(&distributor)),
                Extension(Principal::Unauthenticated),
                tenant_headers(),
            )
            .await,
        )
        .await;
        let tenant_stats = body(
            user_stats(
                State(distributor),
                Extension(Principal::Unauthenticated),
                tenant_headers(),
            )
            .await,
        )
        .await;

        check!(limits["ingestion_rate"] == 10_000.0);
        check!(limits["max_global_series_per_user"] == 150_000);
        check!(tenant_stats["numSeries"] == 0);
    }

    #[tokio::test]
    async fn runtime_config_reports_the_effective_safe_configuration() {
        let response = runtime_config(State(state()), Query(RuntimeConfigQuery::default())).await;
        check!(response.status() == StatusCode::OK);
        check!(response.headers()[CONTENT_TYPE] == "application/yaml");
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let yaml = String::from_utf8(bytes.to_vec()).unwrap();
        check!(yaml.contains("defaults:"));
        check!(yaml.contains("overrides: {}"));
    }
}
