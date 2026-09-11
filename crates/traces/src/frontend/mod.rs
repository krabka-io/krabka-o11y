//! The `query-frontend` role, in front of N queriers.
//!
//! The role covers search sharding, job queueing, querier fan-out, and
//! spanSet/trace merge. Search sharding produces block and row-group jobs plus
//! a live shard.
//!
//! The pipeline composes as
//! `plan jobs -> assign to ready queriers -> queue (bounded fan-out) -> per-job
//! search -> merge (limit/spss) -> render Tempo JSON`. It runs over the typed
//! serde structs in [`wire`] rather than raw `serde_json::Value`.
//!
//! Who the queriers are is [`membership`]'s answer, refreshed from DNS and
//! each querier's own `/ready`. Which querier a shard goes to is
//! [`assignment`]'s, and the two shard kinds get different answers because
//! only one of them is interchangeable work.

pub mod assignment;
pub mod backend;
pub mod config;
pub mod http_backend;
pub mod job;
pub mod membership;
pub mod merge;
pub mod metrics_merge;
pub mod queue;
pub mod server;
pub mod wire;

use std::sync::Arc;

pub use assignment::{AssignedJob, assign_jobs, pick_querier};
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
pub use membership::{
    HttpReadinessProbe, Membership, MembershipView, QUERIER_MEMBERSHIP_GATE, QuerierHealth,
    QuerierMember, ReadinessProbe, refresh_membership, run_membership_refresh,
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
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(catalog),
            cfg,
            MembershipView::fixed(["q1:3200"]),
        );

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
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(catalog),
            cfg,
            MembershipView::fixed(["q1:3200"]),
        );
        let resp = qf.search("t1", "{ }", 0, 300, 1, 3).await.unwrap();
        assert2::assert!(resp.traces.len() == 1);
        assert2::assert!(resp.traces[0].start_time_unix_nano.as_str() == "300");
    }

    #[tokio::test]
    async fn trace_by_id_fans_one_job_per_ready_querier() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
        let backend = MockQuerier::new();
        let cfg = FrontendConfig {
            max_concurrency: 1,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(catalog),
            cfg,
            MembershipView::fixed(["q1:3200", "q2:3200", "q3:3200"]),
        );
        let (_t, metrics, status, warnings) = qf.trace_by_id("t1", [9; 16], 0, 300).await.unwrap();
        // One job per ready querier (3), each addressed by name, none returned
        // the trace => Complete + None.
        check!(
            qf.backend_ref()
                .trace_calls()
                .iter()
                .map(|call| call.querier.clone())
                .collect::<std::collections::BTreeSet<_>>()
                == ["q1:3200", "q2:3200", "q3:3200"]
                    .map(String::from)
                    .into_iter()
                    .collect()
        );
        check!(metrics.total_jobs == 3);
        check!(warnings.is_empty());
        assert2::assert!(matches!(status, TraceStatus::Complete));
    }

    /// The hot tier is sharded across the queriers' live-stores, so a live
    /// shard has to reach all of them. A cold block is readable from any one,
    /// so it goes to exactly one.
    #[tokio::test]
    async fn a_live_shard_reaches_every_querier_and_a_block_reaches_one() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
        let backend = MockQuerier::new();
        let cfg = FrontendConfig {
            hot_frontier_ns: 0,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(catalog),
            cfg,
            MembershipView::fixed(["q1:3200", "q2:3200", "q3:3200"]),
        );
        let resp = qf.search("t1", "{ }", 0, 300, 20, 3).await.unwrap();

        let calls = qf.backend_ref().search_calls();
        let live: std::collections::BTreeSet<String> = calls
            .iter()
            .filter(|c| c.shard == JobShard::Live)
            .map(|c| c.querier.clone())
            .collect();
        let cold: Vec<&SearchJobRequest> =
            calls.iter().filter(|c| c.shard != JobShard::Live).collect();
        check!(live.len() == 3, "every querier holds a different hot slice");
        check!(cold.len() == 1, "the block is in object storage, read once");
        check!(resp.metrics.total_jobs == 4);
        check!(resp.warnings.is_empty(), "nothing was excluded");
    }

    /// A querier that fails its readiness probe is out of the pool before the
    /// query is planned. Its cold blocks move to a querier that is up, so the
    /// cold half of the answer is complete; the recent spans only it held
    /// cannot move anywhere, so the answer says so rather than returning a
    /// fraction as a whole.
    #[tokio::test]
    async fn an_unready_querier_costs_no_blocks_and_is_named_in_the_warnings() {
        let catalog = MockCatalog::new(vec![
            block("b1", 0, 100, &[500]),
            block("b2", 100, 200, &[500]),
        ]);
        let backend = MockQuerier::new();
        let cfg = FrontendConfig {
            hot_frontier_ns: 0,
            ..FrontendConfig::default()
        };
        let membership = MembershipView::empty();
        membership.publish(vec![
            QuerierMember::ready("up:3200"),
            QuerierMember {
                addr: "down:3200".to_string(),
                health: QuerierHealth::NotReady {
                    pending: "live-store".to_string(),
                },
            },
        ]);
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg, membership);

        let resp = qf.search("t1", "{ }", 0, 300, 20, 3).await.unwrap();
        let calls = qf.backend_ref().search_calls();
        check!(
            calls.iter().all(|c| c.querier == "up:3200"),
            "no job is sent to a querier that failed its probe"
        );
        check!(
            calls.iter().filter(|c| c.shard != JobShard::Live).count() == 2,
            "both blocks are still scanned, by the querier that is up"
        );
        check!(resp.warnings.len() == 1);
        check!(resp.warnings[0].contains("down:3200"));
        check!(resp.warnings[0].contains("live-store"));
    }

    /// Nothing ready is a failure, not an empty result: a `200 []` from a pool
    /// with no queriers in it is the completest form of a silently lost answer.
    #[tokio::test]
    async fn a_pool_with_nobody_ready_fails_instead_of_answering_empty() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
        let backend = MockQuerier::new();
        let membership = MembershipView::empty();
        membership.publish(vec![QuerierMember {
            addr: "down:3200".to_string(),
            health: QuerierHealth::Unreachable {
                error: "connection refused".to_string(),
            },
        }]);
        let qf = QueryFrontend::new(
            Arc::new(backend),
            Arc::new(catalog),
            FrontendConfig::default(),
            membership,
        );
        let err = qf.search("t1", "{ }", 0, 300, 20, 3).await.unwrap_err();
        check!(matches!(err, BackendError::Transport(_)));
        check!(qf.backend_ref().search_calls().is_empty());
    }

    /// A backend that republishes the membership the first time it is asked
    /// for a job, which is a refresh landing mid-query.
    struct RefreshingQuerier {
        view: MembershipView,
        refreshed: std::sync::atomic::AtomicBool,
    }

    impl RefreshingQuerier {
        fn refresh_once(&self) {
            if !self
                .refreshed
                .swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                self.view.publish(vec![
                    QuerierMember::ready("q1:3200"),
                    QuerierMember::ready("q2:3200"),
                ]);
            }
        }
    }

    #[async_trait::async_trait]
    impl QuerierBackend for RefreshingQuerier {
        async fn search_job(&self, _: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
            self.refresh_once();
            Ok(SearchPartial::default())
        }
        async fn trace_by_id_job(
            &self,
            _: &TraceByIdJobRequest,
        ) -> Result<TracePartial, BackendError> {
            Ok(TracePartial::default())
        }
        async fn tag_names_job(
            &self,
            _: &TagNamesJobRequest,
        ) -> Result<TagNamesPartial, BackendError> {
            Ok(TagNamesPartial::default())
        }
        async fn tag_values_job(
            &self,
            _: &TagValuesJobRequest,
        ) -> Result<TagValuesPartial, BackendError> {
            Ok(TagValuesPartial::default())
        }
        async fn metrics_job(&self, _: &MetricsJobRequest) -> Result<MetricsPartial, BackendError> {
            Ok(MetricsPartial::default())
        }
    }

    /// A pool that changes between planning and collecting cannot be claimed
    /// to have been covered, so the answer says it may not have been. The
    /// querier that joined after the plan was made was never asked for its
    /// slice of the hot tier, and nothing else can supply it.
    #[tokio::test]
    async fn a_pool_that_changes_mid_query_is_reported_rather_than_ignored() {
        let catalog = MockCatalog::new(vec![block("b1", 0, 100, &[500])]);
        let membership = MembershipView::fixed(["q1:3200"]);
        let backend = RefreshingQuerier {
            view: membership.clone(),
            refreshed: std::sync::atomic::AtomicBool::new(false),
        };
        let cfg = FrontendConfig {
            hot_frontier_ns: 0,
            ..FrontendConfig::default()
        };
        let qf = QueryFrontend::new(Arc::new(backend), Arc::new(catalog), cfg, membership);

        let resp = qf.search("t1", "{ }", 0, 300, 20, 3).await.unwrap();
        check!(resp.warnings.len() == 1, "{:?}", resp.warnings);
        check!(resp.warnings[0].contains("the querier pool changed"));
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
            MembershipView::fixed(["q1:3200"]),
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
            MembershipView::fixed(["q1:3200"]),
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
            MembershipView::fixed(["q1:3200"]),
        );
        let err = qf.tag_values("t1", "span.name", 0, 300).await.unwrap_err();
        assert2::assert!(matches!(err, BackendError::Transport(_)));
    }
}

mod catalog_error;
mod query_frontend;

use catalog_error::catalog_error;
pub use query_frontend::QueryFrontend;
