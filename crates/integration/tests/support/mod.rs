//! The operating-envelope soak's harness: one adapter per signal over that
//! signal's real writer and read path, and the phase runner that drives them.

pub mod config;
pub mod latency;
pub mod logs;
pub mod metrics;
pub mod minio;
pub mod phases;
pub mod profiles;
pub mod report;
pub mod rss;
pub mod runner;
pub mod store_stats;
pub mod traces;

use std::{
    fmt::Display,
    future::Future,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use krabka_blockstore::{LabelPair, Labels};
use object_store::ObjectStore;
use serde_json::Value;
use tokio::sync::Mutex as AsyncMutex;

/// Builds blockstore labels from named test data.
pub fn labels(entries: impl IntoIterator<Item = krabka_integration::LabelEntry>) -> Labels {
    Labels::from_pairs(
        entries
            .into_iter()
            .map(|entry| LabelPair::new(entry.name, entry.value)),
    )
}

/// How long a reader serves from its cached index before it reloads it.
///
/// The queriers poll their index on an interval. One second keeps a phase's
/// reads close to its writes without turning every query into an index load.
pub const REFRESH_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) struct CachedEngine<T> {
    pub loaded_at: Instant,
    pub engine: Arc<T>,
}

pub(crate) async fn cached_engine<T, F, Fut>(
    reader: &AsyncMutex<Option<CachedEngine<T>>>,
    fresh: bool,
    load: F,
) -> Result<Arc<T>, String>
where
    T: Send + Sync,
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<Arc<T>, String>>,
{
    let mut reader = reader.lock().await;
    if let Some(cached) = reader.as_ref()
        && !due(Some(cached.loaded_at), fresh)
    {
        return Ok(Arc::clone(&cached.engine));
    }
    let engine = load().await?;
    *reader = Some(CachedEngine {
        loaded_at: Instant::now(),
        engine: Arc::clone(&engine),
    });
    Ok(engine)
}

/// The tenants the phases write as. The noisy-tenant phase uses the last two.
pub const TENANTS: [&str; 3] = ["soak", "quiet", "noisy"];

/// One write: `series` distinct series, each with `samples` points.
#[derive(Clone, Copy, Debug)]
pub struct Batch {
    /// Unique across the run. The adapters use it as the write-ahead offset.
    pub seq: i64,
    pub series: u32,
    pub samples: u32,
}

impl Batch {
    pub fn rows(self) -> u64 {
        u64::from(self.series) * u64::from(self.samples)
    }
}

/// What the ingest path did with one batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteOutcome {
    Accepted,
    /// The per-tenant rate limiter refused the batch. Nothing reached the
    /// object store.
    Rejected,
}

/// The three views of one phase's object store.
///
/// All three reach the same prefix of the same bucket. Each counts its own
/// requests, so the report can say what a write, a read and a maintenance pass
/// each cost rather than one mixed total.
#[derive(Clone)]
pub struct Stores {
    pub write: Arc<dyn ObjectStore>,
    pub read: Arc<dyn ObjectStore>,
    pub maintenance: Arc<dyn ObjectStore>,
}

/// One signal's ingest, query and maintenance paths, as the soak drives them.
#[async_trait]
pub trait Signal: Send + Sync {
    /// Writes one batch for `tenant` through the signal's block writer and
    /// publishes it the way the block builder does.
    async fn write(&self, tenant: &str, batch: Batch) -> Result<WriteOutcome, String>;

    /// Runs the signal's query for `tenant` over the last `window` and returns
    /// how much data it saw.
    ///
    /// With `fresh`, the reader reloads its index and builds a new engine
    /// first, so no cache from an earlier query serves this one. Without it,
    /// the reader reloads its index at most once per [`REFRESH_INTERVAL`], as
    /// a querier polling its index does.
    async fn query(&self, tenant: &str, window: Duration, fresh: bool) -> Result<u64, String>;

    /// One compaction pass, as the compactor runs it.
    async fn compact(&self) -> Result<Value, String>;

    /// One retention pass that keeps `retention_secs` of data.
    async fn expire(&self, retention_secs: u32) -> Result<Value, String>;

    /// Sets `tenant`'s ingest rate limit, in rows per second.
    fn limit(&self, tenant: &str, rows_per_sec: u32, burst_rows: u64);

    /// How this signal's ingest limit is applied, for the report.
    fn rate_limiter(&self) -> Value;
}

/// The four signals, in report order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignalKind {
    Metrics,
    Logs,
    Traces,
    Profiles,
}

impl SignalKind {
    pub const ALL: [Self; 4] = [Self::Metrics, Self::Logs, Self::Traces, Self::Profiles];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Profiles => "profiles",
        }
    }

    /// Opens the signal over `stores`, loading whatever state they already
    /// hold. A second open over the same stores is a restart.
    pub async fn open(self, stores: Stores, seed: u64) -> Result<Arc<dyn Signal>, String> {
        Ok(match self {
            Self::Metrics => Arc::new(metrics::MetricsSignal::open(stores, seed)),
            Self::Logs => Arc::new(logs::LogsSignal::open(stores, seed).await?),
            Self::Traces => Arc::new(traces::TracesSignal::open(stores, seed)),
            Self::Profiles => Arc::new(profiles::ProfilesSignal::open(stores, seed)),
        })
    }
}

/// Renders any error as the string the report carries.
pub fn err(error: impl Display) -> String {
    error.to_string()
}

/// Whether a reader last refreshed at `last` should refresh again now.
pub fn due(last: Option<std::time::Instant>, fresh: bool) -> bool {
    fresh || last.is_none_or(|at| at.elapsed() >= REFRESH_INTERVAL)
}

/// `window` in whole nanoseconds.
pub fn window_ns(window: Duration) -> i64 {
    i64::try_from(window.as_nanos()).unwrap_or(i64::MAX)
}

/// Wall-clock nanoseconds since the epoch.
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|since| i64::try_from(since.as_nanos()).ok())
        .unwrap_or(0)
}

/// Wall-clock milliseconds since the epoch.
pub fn now_ms() -> i64 {
    now_ns() / 1_000_000
}
#[macro_export]
macro_rules! query_signal_engine {
    ($clock:expr, $start:expr, $body:block) => {
        async fn query(
            &self,
            tenant: &str,
            window: std::time::Duration,
            fresh: bool,
        ) -> Result<u64, String> {
            let engine = self.engine(fresh).await?;
            let end = $clock;
            let start = $start;
            $body
        }
    };
}
