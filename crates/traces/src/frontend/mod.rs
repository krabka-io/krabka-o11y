//! The `query-frontend` role, in front of N queriers.
//!
//! The role covers search sharding, job queueing, querier fan-out, and
//! spanSet/trace merge. Search sharding produces block and row-group jobs plus
//! a live shard.
//!
//! The pipeline composes as
//! `plan jobs -> queue (bounded fan-out) -> per-job search -> merge (limit/spss)
//! -> render Tempo JSON`. It runs over the typed serde structs in [`wire`]
//! rather than raw `serde_json::Value`.

pub mod backend;
pub mod config;
pub mod http_backend;
pub mod job;
pub mod merge;
pub mod metrics_merge;
pub mod queue;
pub mod server;
pub mod wire;

use std::sync::Arc;

pub use backend::{
    BackendError, MetricsJobRequest, MetricsPartial, MockQuerier, QuerierBackend, SearchJobRequest,
    SearchPartial, TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial,
    TraceByIdJobRequest, TracePartial,
};
pub use config::FrontendConfig;
pub use http_backend::{HttpQuerier, run_query_frontend};
pub use job::{
    BlockCatalog, BlockMetaInfo, CatalogError, JobPlan, JobShard, MockCatalog, RowGroupInfo,
    TraceIndexCatalog, blocks_for_tenant, plan_search_jobs,
};
pub use merge::{
    TraceStatus, assemble_trace, assembled_span_count, merge_search, merge_tag_names,
    merge_tag_values,
};
pub use metrics_merge::{
    Exemplar, KeyValue, MetricSample, MetricSeries, MetricsResponseJson, limit_exemplars,
    merge_metric_series, merge_metrics,
};
pub use queue::run_jobs;
pub use server::router_with_backend;
pub use wire::{
    AnyValueJson, ArrayValueJson, KeyValueJson, Metrics, OtlpSpanJson, ResourceSpansJson,
    ScopeSpansJson, SearchResponseJson, SpanJson, SpanSetJson, TraceByIdResponseJson,
    TraceEnvelopeJson, TraceJson, hex8, hex16, parse_hex8, parse_hex16,
};

#[cfg(test)]
mod orch_tests {
    use std::sync::Arc;

    use assert2::check;
    use krabka_units::{ByteSize, bytes, convert::ByteSizeExt as _, millis};

    use super::*;
    use crate::frontend::{
        backend::{MockQuerier, SearchPartial},
        job::{BlockMetaInfo, MockCatalog, RowGroupInfo},
        wire::{Metrics, SpanJson, SpanSetJson, TraceJson},
    };

    fn block(id: &str, start: i64, end: i64, rgs: &[u64]) -> BlockMetaInfo {
        let row_groups = rgs
            .iter()
            .enumerate()
            .map(|(i, &b)| RowGroupInfo {
                index: u32::try_from(i).unwrap(),
                compressed: ByteSize::from_bytes(b),
            })
            .collect();
        BlockMetaInfo {
            block_id: id.to_string(),
            start_ns: start,
            end_ns: end,
            size: ByteSize::from_bytes(rgs.iter().sum()),
            row_groups,
        }
    }

    fn one_trace(tid: &str, start: u64) -> SearchPartial {
        SearchPartial {
            traces: vec![TraceJson {
                trace_id: tid.to_string(),
                root_service_name: "svc".to_string(),
                root_trace_name: "GET /".to_string(),
                start_time_unix_nano: start.to_string(),
                duration: millis(1),
                span_sets: vec![SpanSetJson {
                    spans: vec![SpanJson {
                        span_id: tid.to_string(),
                        start_time_unix_nano: start.to_string(),
                        duration_nanos: "1".to_string(),
                        attributes: vec![],
                    }],
                    matched: 1,
                }],
            }],
            metrics: Metrics {
                completed_jobs: 1,
                inspected_bytes: 100,
                inspected_traces: 1,
                inspected_spans: 1,
                ..Metrics::default()
            },
        }
    }

    #[tokio::test]
    async fn search_plans_jobs_fans_and_merges() {
        // Two small cold blocks + a hot window => 1 Live + 2 block jobs = 3.
        let catalog = MockCatalog::new(vec![
            block("b1", 0, 100, &[500]),
            block("b2", 100, 200, &[500]),
        ]);
        let backend = MockQuerier::new();
        backend.stub_search(one_trace("01", 50));
        backend.stub_search(one_trace("02", 150));
        backend.stub_search(one_trace("03", 250));
        let cfg = FrontendConfig {
            target_per_job: bytes(10_000),
            max_concurrency: 1,
            hot_frontier_ns: 150,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);

        let resp = qf.search("t1", "{ }", 0, 300, 20, 3).await.unwrap();
        assert2::assert!(qf.backend_ref().search_calls().len() == 3);
        assert2::assert!(
            qf.backend_ref()
                .search_calls()
                .iter()
                .map(|call| call.tenant.as_str())
                .collect::<Vec<_>>()
                == vec!["t1", "t1", "t1"]
        );
        assert2::assert!(resp.traces.len() == 3);
        // A successful multi-job search folds real per-job accounting:
        // completedJobs == totalJobs, and non-zero inspected traces/spans (not
        // the all-zero block that the querier used to emit).
        check!(
            resp.metrics
                == Metrics {
                    total_jobs: 3,
                    completed_jobs: 3,
                    total_blocks: 2,
                    inspected_traces: 3,
                    inspected_bytes: 300,
                    inspected_spans: 3,
                }
        );
    }

    #[tokio::test]
    async fn search_honors_limit() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
        let backend = MockQuerier::new();
        backend.stub_search(SearchPartial {
            traces: vec![
                one_trace("01", 100).traces.pop().unwrap(),
                one_trace("02", 300).traces.pop().unwrap(),
                one_trace("03", 200).traces.pop().unwrap(),
            ],
            metrics: Metrics {
                completed_jobs: 1,
                ..Metrics::default()
            },
        });
        let cfg = FrontendConfig {
            hot_frontier_ns: i64::MAX,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
        let resp = qf.search("t1", "{ }", 0, 300, 1, 3).await.unwrap();
        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].start_time_unix_nano.as_str() == "300");
    }

    #[tokio::test]
    async fn trace_by_id_fans_one_job_per_querier() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
        let backend = MockQuerier::with_querier_count(3);
        let cfg = FrontendConfig {
            max_concurrency: 1,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg);
        let (_t, metrics, status) = qf.trace_by_id("t1", [9; 16], 0, 300).await.unwrap();
        // One job per querier (3), none returned the trace => Complete + None.
        check!(qf.backend_ref().trace_calls().len() == 3);
        check!(metrics.total_jobs == 3);
        assert2::assert!(matches!(status, TraceStatus::Complete));
    }

    /// A catalog whose enumeration always fails, so a partition is
    /// unreachable.
    struct FailingCatalog;

    #[async_trait::async_trait]
    impl crate::frontend::job::BlockCatalog for FailingCatalog {
        async fn blocks(
            &self,
            _tenant: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<BlockMetaInfo>, crate::frontend::job::CatalogError> {
            Err(crate::frontend::job::CatalogError::Backend(
                "partition unreachable".to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn search_surfaces_catalog_error_instead_of_empty_200() {
        // A catalog failure drops the cold partitions; swallowing it would return
        // a misleading live-only 200. It must surface as a backend error (5xx).
        let backend = MockQuerier::new();
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(FailingCatalog),
            FrontendConfig::default(),
        );
        let err = qf.search("t1", "{ }", 0, 300, 20, 3).await.unwrap_err();
        assert2::assert!(matches!(err, BackendError::Transport(_)));
        // The backend was never fanned out — the catalog error short-circuits.
        assert2::assert!(qf.backend_ref().search_calls().is_empty());
    }

    #[tokio::test]
    async fn tag_names_surfaces_catalog_error_instead_of_empty_200() {
        let backend = MockQuerier::new();
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(FailingCatalog),
            FrontendConfig::default(),
        );
        let err = qf.tag_names("t1", None, 0, 300).await.unwrap_err();
        assert2::assert!(matches!(err, BackendError::Transport(_)));
    }

    #[tokio::test]
    async fn tag_values_surfaces_catalog_error_instead_of_empty_200() {
        let backend = MockQuerier::new();
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(FailingCatalog),
            FrontendConfig::default(),
        );
        let err = qf.tag_values("t1", "span.name", 0, 300).await.unwrap_err();
        assert2::assert!(matches!(err, BackendError::Transport(_)));
    }
}

// === split-modules: generated submodules ===
mod catalog_error;
mod query_frontend;

use catalog_error::catalog_error;
pub use query_frontend::QueryFrontend;
