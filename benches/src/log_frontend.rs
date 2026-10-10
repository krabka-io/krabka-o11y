//! Complete frontend requests with warmed tenant indexes and uncached results.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    http::{Request, StatusCode},
};
use krabka_observability::{
    QuerierIndexSource, Role, ServiceConfig, ServiceDependencies, build_service_router,
};
use krabka_units::{bytes, hours, secs};
use object_store::local::LocalFileSystem;
use serde_json::{Value, json};
use tower::ServiceExt as _;

use crate::log_queries::{LogQueryFixture, POINTS_PER_STREAM};

/// Requests that distinguish preparation cost from broad result construction.
pub const CASES: [&str; 7] = [
    "all",
    "all_sharded",
    "sparse",
    "rare",
    "single",
    "none",
    "label_values",
];
const TENANT: &str = "log-query-bench";
static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

struct FixtureRoot(PathBuf);

impl Drop for FixtureRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Real Parquet, tenant manifests and production HTTP handlers on local storage.
pub struct LogFrontendFixture {
    queries: LogQueryFixture,
    routers: [Router; 2],
    limit: usize,
    _root: FixtureRoot,
}

impl LogFrontendFixture {
    /// Create the same data as the engine fixture and configure uncached queries.
    ///
    /// # Panics
    /// Panics if fixture persistence or service construction fails.
    pub async fn new(streams: usize) -> Self {
        Self::with_index_source(streams, QuerierIndexSource::TenantObjectStoreManifest).await
    }

    /// Cache the persisted shard but prepare a fresh merged index per request.
    ///
    /// # Panics
    /// Panics if fixture persistence or service construction fails.
    pub async fn new_shards(streams: usize) -> Self {
        Self::with_index_source(streams, QuerierIndexSource::TenantObjectStoreShards).await
    }

    async fn with_index_source(streams: usize, source: QuerierIndexSource) -> Self {
        let root = FixtureRoot(std::env::temp_dir().join(format!(
            "krabka-log-frontend-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed),
        )));
        std::fs::create_dir(&root.0).expect("a private fixture directory creates");
        let store =
            Arc::new(LocalFileSystem::new_with_prefix(&root.0).expect("the fixture store opens"));
        let queries = LogQueryFixture::new_in_store(streams, store).await;
        if source == QuerierIndexSource::TenantObjectStoreShards {
            queries.persist_shard().await;
        } else {
            queries.persist_indexes().await;
        }
        let limits = root.0.join("limits.yaml");
        std::fs::write(
            &limits,
            "defaults:\n  max_entries_limit_per_query: 0\n  max_query_series: 0\n",
        )
        .expect("fixture query limits write");
        let config = ServiceConfig {
            target: Role::Querier,
            data_root: root.0.join("cache"),
            object_store_url: Some(format!("file://{}", root.0.display())),
            index_prefix: Some(queries.prefix.to_string()),
            querier_index_source: source,
            logs_limits_overrides_config: Some(limits),
            querier_dynamic_index_cache_ttl: if source
                == QuerierIndexSource::TenantObjectStoreShards
            {
                secs(0)
            } else {
                hours(1)
            },
            querier_shard_index_cache_ttl: hours(1),
            querier_query_frontend_cache_ttl: secs(0),
            ..ServiceConfig::default()
        };
        let dependencies = ServiceDependencies::default();
        let plain = build_service_router(&config, dependencies.clone(), None)
            .await
            .expect("the fixture frontend builds");
        let sharded = build_service_router(
            &ServiceConfig {
                querier_query_frontend_target_bytes_per_shard: bytes(
                    u32::try_from(queries.block_bytes().div_ceil(4).max(1))
                        .expect("the fixture shard budget fits u32"),
                ),
                ..config
            },
            dependencies,
            None,
        )
        .await
        .expect("the sharded fixture frontend builds");
        Self {
            queries,
            routers: [plain, sharded],
            limit: streams * POINTS_PER_STREAM,
            _root: root,
        }
    }

    /// Execute and consume a complete serialized response.
    ///
    /// # Errors
    /// Returns an error for an HTTP failure or an unreadable response body.
    ///
    /// # Panics
    /// Panics for an unknown case or an invalid fixture request URI.
    pub async fn execute(&self, name: &str) -> Result<Bytes, String> {
        let selector = match name {
            "all" | "all_sharded" => "%7Bapp%21%3D%22%22%7D",
            "sparse" => "%7Bapp%3D%22api%22%7D",
            "rare" => "%7Bbucket%3D%22rare%22%7D",
            "single" => "%7Bseries%3D%2200000000%22%7D",
            "none" => "%7Bapp%3D%22absent%22%7D",
            "label_values" => "",
            _ => panic!("unknown frontend case"),
        };
        let uri = if name == "label_values" {
            "/loki/api/v1/label/app/values?start=0.000000000&end=0.000000010".to_string()
        } else {
            format!(
                "/loki/api/v1/query_range?query={selector}&start=0.000000000&end=0.000000010&direction=forward&limit={}",
                self.limit,
            )
        };
        let response = self.routers[usize::from(name == "all_sharded")]
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("X-Scope-OrgID", TENANT)
                    .body(Body::empty())
                    .expect("a fixture request builds"),
            )
            .await
            .expect("a fixture request executes");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .map_err(|error| error.to_string())?;
        if status != StatusCode::OK {
            return Err(format!(
                "fixture request failed with {status}: {}",
                String::from_utf8_lossy(&body)
            ));
        }
        Ok(body)
    }

    /// Verify every returned row and label against the independent input ledger.
    ///
    /// The API envelope and complete result payload are compared; execution
    /// statistics are omitted because request timing and cache counters vary.
    ///
    /// # Errors
    /// Returns an error if the response is malformed or differs from the ledger.
    ///
    /// # Panics
    /// Panics for an unknown fixture case.
    pub async fn verify(&self, name: &str) -> Result<(), String> {
        let actual = self.execute(name).await?;
        let mut actual: Value =
            serde_json::from_slice(&actual).map_err(|error| error.to_string())?;
        if let Some(data) = actual["data"].as_object_mut() {
            data.remove("stats");
        }
        let expected = match name {
            "all" | "all_sharded" => self.queries.case("all_exact").expected,
            "sparse" | "rare" | "single" => self.queries.case(name).expected,
            "none" => json!({"status":"success", "data":{"resultType":"streams", "result":[]}}),
            "label_values" => json!({"status":"success", "data":["api", "worker"]}),
            _ => panic!("unknown frontend case"),
        };
        if actual != expected {
            return Err("the complete frontend response differs from the input ledger".to_string());
        }
        Ok(())
    }
}
