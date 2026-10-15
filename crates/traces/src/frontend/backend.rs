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
    job::{CatalogError, JobShard},
    metrics_merge::MetricsResponseJson,
    wire::{Metrics, TraceByIdResponseJson, TraceJson},
};

/// Map a block-catalog enumeration failure to a backend transport error.
///
/// The endpoint then surfaces a 5xx instead of a silent return of only
/// live-tier results. A search or tag query partitions data across the live
/// tier and disjoint cold blocks, so swallowing a catalog error would return a
/// misleading `200` with the cold partitions missing.
pub(crate) fn catalog_error(err: &CatalogError) -> BackendError {
    BackendError::Transport(err.to_string())
}

/// Failure modes of a single backend job.
#[derive(Clone, Debug, thiserror::Error)]
pub enum BackendError {
    #[error("backend job timed out")]
    Timeout,
    #[error("backend transport error: {0}")]
    Transport(String),
    #[error("backend returned error ({status}): {message}")]
    Backend { status: String, message: String },
    #[error("query frontend overloaded; retry after {retry_after_seconds}s")]
    Overloaded { retry_after_seconds: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BackendHttpError {
    pub status: u16,
    pub body: String,
}

impl BackendError {
    /// Map this backend failure to the status and body the frontend returns to
    /// its client.
    ///
    /// This keeps the upstream querier's status code and error text where they
    /// are known. A timeout becomes `504`, and a transport failure becomes
    /// `502`.
    #[must_use]
    pub fn to_http(&self) -> BackendHttpError {
        match self {
            BackendError::Timeout => BackendHttpError {
                status: 504,
                body: "backend job timed out".to_string(),
            },
            BackendError::Transport(detail) => BackendHttpError {
                status: 502,
                body: format!("backend transport error: {detail}"),
            },
            BackendError::Backend { status, message } => BackendHttpError {
                status: status.parse::<u16>().unwrap_or(502),
                body: message.clone(),
            },
            BackendError::Overloaded {
                retry_after_seconds,
            } => BackendHttpError {
                status: 429,
                body: format!("query frontend overloaded; retry after {retry_after_seconds}s"),
            },
        }
    }
}

/// A `TraceQL`-metrics job over a window with a step.
///
/// The job is `/api/metrics/query_range` or `/api/metrics/query`. `instant`
/// selects the instant-query path.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricsJobRequest {
    /// The resolved tenant, which the transport sends as `X-Scope-OrgID`.
    pub tenant: TenantId,
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: i64,
    pub instant: bool,
    pub shard: JobShard,
    /// The `host:port` of the querier this job is assigned to.
    ///
    /// The frontend assigns every job to a querier it has just seen ready, so
    /// a backend never chooses one itself and never dials an address that the
    /// membership has already ejected.
    pub querier: String,
}

/// The partial result of one metrics job: the series body plus the accounting.
#[derive(Clone, Debug, Default)]
pub struct MetricsPartial {
    pub response: MetricsResponseJson,
    pub metrics: Metrics,
}

/// A single search job: a `TraceQL` search over a window, restricted to one
/// shard. That shard is the live hot tier, or one cold block narrowed to a
/// row-group range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchJobRequest {
    /// The resolved tenant, which the transport sends as `X-Scope-OrgID`.
    pub tenant: TenantId,
    pub query: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub limit: usize,
    pub spss: usize,
    pub shard: JobShard,
    /// The `host:port` of the querier this job is assigned to.
    ///
    /// The frontend assigns every job to a querier it has just seen ready, so
    /// a backend never chooses one itself and never dials an address that the
    /// membership has already ejected.
    pub querier: String,
}

/// The partial result of one search job: the matched traces as typed Tempo
/// JSON, plus the job's accounting.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SearchPartial {
    pub traces: Vec<TraceJson>,
    pub metrics: Metrics,
}

/// A tag-names job for one optional scope over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagNamesJobRequest {
    /// The resolved tenant, which the transport sends as `X-Scope-OrgID`.
    pub tenant: TenantId,
    pub scope: Option<TagScope>,
    pub start_ns: i64,
    pub end_ns: i64,
    pub shard: JobShard,
    /// The `host:port` of the querier this job is assigned to.
    ///
    /// The frontend assigns every job to a querier it has just seen ready, so
    /// a backend never chooses one itself and never dials an address that the
    /// membership has already ejected.
    pub querier: String,
}

/// The partial result of one tag-names job.
#[derive(Clone, Debug, Default)]
pub struct TagNamesPartial {
    pub tags: Vec<ScopedTag>,
    pub metrics: Metrics,
}

/// A tag-values job for one tag over a window.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TagValuesJobRequest {
    /// The resolved tenant, which the transport sends as `X-Scope-OrgID`.
    pub tenant: TenantId,
    pub tag: String,
    pub start_ns: i64,
    pub end_ns: i64,
    pub shard: JobShard,
    /// The `host:port` of the querier this job is assigned to.
    ///
    /// The frontend assigns every job to a querier it has just seen ready, so
    /// a backend never chooses one itself and never dials an address that the
    /// membership has already ejected.
    pub querier: String,
}

/// The partial result of one tag-values job.
#[derive(Clone, Debug, Default)]
pub struct TagValuesPartial {
    pub values: Vec<TypedValue>,
    pub metrics: Metrics,
}

/// A by-id job: fetch one trace's spans from one querier.
///
/// By-id does **not** fan per-block, because the querier reassembles a trace
/// across blocks. The frontend fans one job per querier and unions their v2
/// responses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceByIdJobRequest {
    /// The resolved tenant, which the transport sends as `X-Scope-OrgID`.
    pub tenant: TenantId,
    pub trace_id: [u8; 16],
    pub start_ns: i64,
    pub end_ns: i64,
    /// The `host:port` of the querier this job is assigned to, so that a
    /// fan-out queries each ready querier exactly once.
    ///
    /// It is an address rather than an index into a pool: the pool changes
    /// between refreshes, and an index into a list that has since shrunk names
    /// a different querier than the one it was chosen for.
    pub querier: String,
}

/// The partial result of one by-id job: the typed v2 trace body, which may be
/// empty, plus the accounting.
#[derive(Clone, Debug, Default)]
pub struct TracePartial {
    pub trace: TraceByIdResponseJson,
    pub metrics: Metrics,
}


/// A programmable in-process backend for tests.
///
/// It returns the next stubbed response, FIFO, and the last stub repeats if
/// more calls arrive. It records every request for assertions, including the
/// querier each was assigned to. [`MockQuerier::fail_querier`] makes one
/// address behave like a querier that has died between the probe and the job.
///
/// This type is un-gated so that integration tests in `tests/` can construct
/// it. It is a fixture, not production wiring.
pub struct MockQuerier {
    pub(crate) dead: Mutex<BTreeSet<String>>,
    pub(crate) search_stubs: Mutex<Vec<SearchPartial>>,
    pub(crate) trace_stubs: Mutex<Vec<TracePartial>>,
    pub(crate) tag_names_stubs: Mutex<Vec<TagNamesPartial>>,
    pub(crate) tag_values_stubs: Mutex<Vec<TagValuesPartial>>,
    pub(crate) metrics_stubs: Mutex<Vec<MetricsPartial>>,
    pub(crate) search_calls: Mutex<Vec<SearchJobRequest>>,
    pub(crate) trace_calls: Mutex<Vec<TraceByIdJobRequest>>,
    pub(crate) tag_names_calls: Mutex<Vec<TagNamesJobRequest>>,
    pub(crate) tag_values_calls: Mutex<Vec<TagValuesJobRequest>>,
    pub(crate) metrics_calls: Mutex<Vec<MetricsJobRequest>>,
}

impl MockQuerier {
    #[must_use]
    pub fn new() -> Self {
        Self {
            dead: Mutex::new(BTreeSet::new()),
            search_stubs: Mutex::new(Vec::new()),
            trace_stubs: Mutex::new(Vec::new()),
            tag_names_stubs: Mutex::new(Vec::new()),
            tag_values_stubs: Mutex::new(Vec::new()),
            metrics_stubs: Mutex::new(Vec::new()),
            search_calls: Mutex::new(Vec::new()),
            trace_calls: Mutex::new(Vec::new()),
            tag_names_calls: Mutex::new(Vec::new()),
            tag_values_calls: Mutex::new(Vec::new()),
            metrics_calls: Mutex::new(Vec::new()),
        }
    }

    /// Make every job assigned to `addr` fail, as a querier does when it dies
    /// after the membership last saw it ready.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn fail_querier(&self, addr: impl Into<String>) {
        self.dead.lock().unwrap().insert(addr.into());
    }

    /// `Err` when `addr` has been killed with [`MockQuerier::fail_querier`].
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub(crate) fn refuse(&self, addr: &str) -> Result<(), BackendError> {
        if self.dead.lock().unwrap().contains(addr) {
            return Err(BackendError::Transport(format!(
                "connection refused by {addr}"
            )));
        }
        Ok(())
    }

    /// Enqueue a canned search-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_search(&self, p: SearchPartial) {
        self.search_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned by-id-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_trace(&self, p: TracePartial) {
        self.trace_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned tag-names-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_tag_names(&self, p: TagNamesPartial) {
        self.tag_names_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned tag-values-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_tag_values(&self, p: TagValuesPartial) {
        self.tag_values_stubs.lock().unwrap().push(p);
    }

    /// Enqueue a canned metrics-job response, FIFO.
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn stub_metrics(&self, p: MetricsPartial) {
        self.metrics_stubs.lock().unwrap().push(p);
    }

    /// All recorded search-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn search_calls(&self) -> Vec<SearchJobRequest> {
        self.search_calls.lock().unwrap().clone()
    }

    /// All recorded by-id-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn trace_calls(&self) -> Vec<TraceByIdJobRequest> {
        self.trace_calls.lock().unwrap().clone()
    }

    /// All recorded tag-names-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn tag_names_calls(&self) -> Vec<TagNamesJobRequest> {
        self.tag_names_calls.lock().unwrap().clone()
    }

    /// All recorded tag-values-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn tag_values_calls(&self) -> Vec<TagValuesJobRequest> {
        self.tag_values_calls.lock().unwrap().clone()
    }

    /// All recorded metrics-job requests, in dispatch order.
    #[must_use]
    ///
    /// # Panics
    /// Panics if an internal synchronization primitive is poisoned.
    pub fn metrics_calls(&self) -> Vec<MetricsJobRequest> {
        self.metrics_calls.lock().unwrap().clone()
    }

    pub(crate) fn pop<T: Clone + Default>(stubs: &Mutex<Vec<T>>) -> T {
        let mut s = stubs.lock().unwrap();
        if s.len() > 1 {
            s.remove(0)
        } else {
            s.first().cloned().unwrap_or_default()
        }
    }
}

impl Default for MockQuerier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl QuerierBackend for MockQuerier {
    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError> {
        self.search_calls.lock().unwrap().push(req.clone());
        self.refuse(&req.querier)?;
        Ok(Self::pop(&self.search_stubs))
    }

    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError> {
        self.trace_calls.lock().unwrap().push(req.clone());
        self.refuse(&req.querier)?;
        Ok(Self::pop(&self.trace_stubs))
    }

    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError> {
        self.tag_names_calls.lock().unwrap().push(req.clone());
        self.refuse(&req.querier)?;
        Ok(Self::pop(&self.tag_names_stubs))
    }

    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError> {
        self.tag_values_calls.lock().unwrap().push(req.clone());
        self.refuse(&req.querier)?;
        Ok(Self::pop(&self.tag_values_stubs))
    }

    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError> {
        self.metrics_calls.lock().unwrap().push(req.clone());
        self.refuse(&req.querier)?;
        Ok(Self::pop(&self.metrics_stubs))
    }
}

/// A transport that runs one assigned job against one querier.
///
/// Every method is one fanned-out job's worth of work, and every request
/// carries the `host:port` it is for. The backend chooses nothing: which
/// queriers exist and which of them may take work is
/// [`MembershipView`](crate::frontend::membership::MembershipView)'s answer,
/// and where a shard goes is [`assign_jobs`](crate::frontend::assign_jobs)'.
/// A backend that picked its own target would be picking from a list nobody
/// was keeping current, which is what it used to do.
#[async_trait]
pub trait QuerierBackend: Send + Sync {
    async fn search_job(&self, req: &SearchJobRequest) -> Result<SearchPartial, BackendError>;
    async fn trace_by_id_job(
        &self,
        req: &TraceByIdJobRequest,
    ) -> Result<TracePartial, BackendError>;
    async fn tag_names_job(
        &self,
        req: &TagNamesJobRequest,
    ) -> Result<TagNamesPartial, BackendError>;
    async fn tag_values_job(
        &self,
        req: &TagValuesJobRequest,
    ) -> Result<TagValuesPartial, BackendError>;
    async fn metrics_job(&self, req: &MetricsJobRequest) -> Result<MetricsPartial, BackendError>;
}

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

    fn search_job_request(query: &str, querier: &str) -> SearchJobRequest {
        SearchJobRequest {
            tenant: TenantId::new("t1").unwrap(),
            query: query.to_string(),
            start_ns: 0,
            end_ns: 100,
            limit: 20,
            spss: 3,
            shard: JobShard::Live,
            querier: querier.to_string(),
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
        let req = search_job_request("{ .service.name = \"checkout\" }", "q1:3200");
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
        let req = search_job_request("{ }", "q1:3200");
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
        let job = |addr: &str| search_job_request("{ }", addr);
        assert2::assert!(mock.search_job(&job("live:3200")).await.is_ok());
        let err = mock.search_job(&job("dead:3200")).await.unwrap_err();
        assert2::assert!(matches!(err, BackendError::Transport(_)));
    }
}
