//! The querier-backend abstraction the frontend fans out to, one call per
//! planned job.
//!
//! Tests use [`MockQuerier`]. Real deployments use
//! [`crate::frontend::http_backend::HttpQuerier`].
//!
//! The **typed serde edge model** in [`crate::frontend::wire`] carries the
//! partials, not raw `serde_json::Value`. A search job returns `Vec<TraceJson>`
//! and `Metrics`. A by-id job returns a typed OTLP-JSON
//! `TraceByIdResponseJson`. Tag jobs return the typed tag bodies. The merge
//! layer in `merge.rs` operates on these.

use std::{collections::BTreeSet, sync::Mutex};

use async_trait::async_trait;
use krabka_blockstore::TenantId;
use krabka_traceql::{ScopedTag, TagScope, TypedValue};

use crate::frontend::{
    job::JobShard,
    metrics_merge::MetricsResponseJson,
    wire::{Metrics, TraceByIdResponseJson, TraceJson},
};

#[cfg(test)]
mod tests {
    use krabka_units::millis;

    use super::*;
    use crate::frontend::{job::JobShard, wire::TraceJson};

    fn trace(svc: &str) -> TraceJson {
        TraceJson {
            trace_id: "01".repeat(16),
            root_service_name: svc.to_string(),
            root_trace_name: "GET /".to_string(),
            start_time_unix_nano: "1".to_string(),
            duration: millis(1),
            span_sets: vec![],
        }
    }

    #[tokio::test]
    async fn mock_returns_canned_and_records_calls() {
        let mock = MockQuerier::new();
        mock.stub_search(SearchPartial {
            traces: vec![trace("checkout")],
            metrics: Metrics {
                total_jobs: 1,
                completed_jobs: 1,
                inspected_bytes: 10,
                ..Metrics::default()
            },
        });
        let req = SearchJobRequest {
            tenant: TenantId::new("t1").unwrap(),
            query: "{ .service.name = \"checkout\" }".to_string(),
            start_ns: 0,
            end_ns: 100,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
            querier: "q1:3200".to_string(),
        };
        let out = mock.search_job(&req).await.unwrap();
        assert2::assert!(
            out == SearchPartial {
                traces: vec![trace("checkout")],
                metrics: Metrics {
                    total_jobs: 1,
                    completed_jobs: 1,
                    total_blocks: 0,
                    inspected_traces: 0,
                    inspected_bytes: 10,
                    inspected_spans: 0,
                },
            }
        );
        assert2::assert!(mock.search_calls().len() == 1);
        assert2::assert!(mock.search_calls()[0].tenant.as_str() == "t1");
        assert2::assert!(matches!(mock.search_calls()[0].shard, JobShard::Live));
        assert2::assert!(mock.search_calls()[0].querier.as_str() == "q1:3200");
    }

    #[tokio::test]
    async fn empty_stub_yields_default_partial() {
        let mock = MockQuerier::new();
        let req = SearchJobRequest {
            tenant: TenantId::new("t1").unwrap(),
            query: "{ }".to_string(),
            start_ns: 0,
            end_ns: 100,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
            querier: "q1:3200".to_string(),
        };
        let out = mock.search_job(&req).await.unwrap();
        assert2::assert!(out.traces == vec![]);
        assert2::assert!(out.metrics == Metrics::default());
    }

    /// A querier that dies between the readiness probe and the job refuses
    /// only its own work. The mock models that so a fan-out test can lose one
    /// querier without losing the backend.
    #[tokio::test]
    async fn a_killed_querier_refuses_only_its_own_jobs() {
        let mock = MockQuerier::new();
        mock.fail_querier("dead:3200");
        let job = |addr: &str| SearchJobRequest {
            tenant: TenantId::new("t1").unwrap(),
            query: "{ }".to_string(),
            start_ns: 0,
            end_ns: 100,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
            querier: addr.to_string(),
        };
        assert2::assert!(mock.search_job(&job("live:3200")).await.is_ok());
        let err = mock.search_job(&job("dead:3200")).await.unwrap_err();
        assert2::assert!(matches!(err, BackendError::Transport(_)));
    }
}

mod backend_error;
mod metrics_job_request;
mod metrics_partial;
mod mock_querier;
mod querier_backend;
mod search_job_request;
mod search_partial;
mod tag_names_job_request;
mod tag_names_partial;
mod tag_values_job_request;
mod tag_values_partial;
mod trace_by_id_job_request;
mod trace_partial;

pub use backend_error::BackendError;
pub use metrics_job_request::MetricsJobRequest;
pub use metrics_partial::MetricsPartial;
pub use mock_querier::MockQuerier;
pub use querier_backend::QuerierBackend;
pub use search_job_request::SearchJobRequest;
pub use search_partial::SearchPartial;
pub use tag_names_job_request::TagNamesJobRequest;
pub use tag_names_partial::TagNamesPartial;
pub use tag_values_job_request::TagValuesJobRequest;
pub use tag_values_partial::TagValuesPartial;
pub use trace_by_id_job_request::TraceByIdJobRequest;
pub use trace_partial::TracePartial;
