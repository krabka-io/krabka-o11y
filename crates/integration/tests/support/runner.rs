//! Drives one phase: writers, readers and a maintenance loop against one
//! signal, for a warm-up and then a measured window.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::{Value, json};

use super::{
    Batch, Signal, WriteOutcome,
    config::{Config, Objectives, duration_us},
    latency::{Latencies, as_f64, ratio},
    rss::RssSampler,
    store_stats::StoreMeters,
};

/// How many error messages one phase keeps for the report.
const ERROR_SAMPLES: usize = 5;

/// What the harness does not measure, and why. Every entry carries it, so a
/// reader of the report never mistakes an absent number for a zero.
pub fn wal_lag() -> Value {
    json!({
        "status": "not_measured",
        "reason": "the soak writes blocks straight to the object store through each signal's \
                   block writer; no write-ahead log or broker is in the path, so there is no \
                   consumer lag to read",
    })
}

/// One group of identical writers.
#[derive(Clone, Copy, Debug)]
pub struct Writers {
    pub tenant: &'static str,
    pub count: u32,
    pub series: u32,
    /// Sleep [`Config::write_interval`] between writes. An unpaced writer
    /// writes as fast as the path accepts.
    pub paced: bool,
}

/// One group of identical readers.
#[derive(Clone, Copy, Debug)]
pub struct Readers {
    pub tenant: &'static str,
    pub count: u32,
    pub window: Duration,
    /// Reload the index and build a new engine before every query.
    pub fresh: bool,
    /// The gap between two queries from one reader.
    pub interval: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Maintenance {
    Compact,
    Expire(u32),
}

/// Everything one phase runs at once.
#[derive(Clone, Debug)]
pub struct Plan {
    pub writers: Vec<Writers>,
    pub readers: Vec<Readers>,
    pub maintenance: Option<Maintenance>,
    /// The tenant whose numbers are the entry's top-level numbers. The other
    /// tenants' numbers are under `tenants`.
    pub primary: &'static str,
    pub warmup: Duration,
    pub duration: Duration,
}

/// The shared state of one signal's run: the object store counters, and the
/// sequence that makes every batch's offset unique.
#[derive(Clone)]
pub struct Context {
    pub signal: Arc<dyn Signal>,
    pub meters: StoreMeters,
    pub seq: Arc<AtomicI64>,
    pub config: Arc<Config>,
}

#[derive(Default)]
struct TenantStats {
    write_latency: Latencies,
    query_latency: Latencies,
    accepted_batches: u64,
    rejected_batches: u64,
    accepted_rows: u64,
    rejected_rows: u64,
    write_errors: u64,
    queries: u64,
    query_errors: u64,
    rows_seen: u64,
}

#[derive(Default)]
struct MaintenanceStats {
    passes: u64,
    errors: u64,
    latency: Latencies,
    last: Option<Value>,
}

#[derive(Default)]
struct Stats {
    tenants: BTreeMap<&'static str, TenantStats>,
    maintenance: MaintenanceStats,
    error_samples: Vec<String>,
}

impl Stats {
    fn tenant(&mut self, tenant: &'static str) -> &mut TenantStats {
        self.tenants.entry(tenant).or_default()
    }

    fn error(&mut self, message: String) {
        if self.error_samples.len() < ERROR_SAMPLES {
            self.error_samples.push(message);
        }
    }
}

/// The clock one phase measures against.
///
/// No loop starts an operation after `until`, but an operation that started
/// before it runs to its end. The harness counts every operation that ends
/// after `measure_from`, so a slow operation is measured whole and is never
/// dropped for straddling an edge of the window.
#[derive(Clone, Copy)]
struct Window {
    measure_from: Instant,
    until: Instant,
}

impl Window {
    /// The offset into the measured window of an operation that ended at
    /// `ended`, or `None` when it ended during warm-up.
    fn offset(self, ended: Instant) -> Option<Duration> {
        ended.checked_duration_since(self.measure_from)
    }

    fn open(self) -> bool {
        Instant::now() < self.until
    }
}

/// Runs `plan` and returns the entry for it, without the signal and phase
/// names, which the caller adds.
pub async fn run(ctx: &Context, plan: &Plan) -> Value {
    let start = Instant::now();
    let window = Window {
        measure_from: start + plan.warmup,
        until: start + plan.warmup + plan.duration,
    };
    let mut initial = Stats::default();
    // Every tenant the plan drives has an entry, so a tenant whose every
    // operation failed or never ended reports zeros, not a missing object.
    for tenant in plan
        .writers
        .iter()
        .map(|w| w.tenant)
        .chain(plan.readers.iter().map(|r| r.tenant))
    {
        initial.tenant(tenant);
    }
    let stats = Arc::new(Mutex::new(initial));
    let mut tasks = Vec::new();

    for group in &plan.writers {
        for _ in 0..group.count {
            tasks.push(tokio::spawn(write_loop(
                ctx.clone(),
                *group,
                window,
                Arc::clone(&stats),
            )));
        }
    }
    for group in &plan.readers {
        for _ in 0..group.count {
            tasks.push(tokio::spawn(read_loop(
                ctx.clone(),
                *group,
                window,
                Arc::clone(&stats),
            )));
        }
    }
    if let Some(maintenance) = plan.maintenance {
        tasks.push(tokio::spawn(maintenance_loop(
            ctx.clone(),
            maintenance,
            window,
            Arc::clone(&stats),
        )));
    }

    tokio::time::sleep_until(window.measure_from.into()).await;
    let meters_before = ctx.meters.snapshot();
    let rss = RssSampler::start();
    for task in tasks {
        task.await.expect("a soak task does not panic");
    }
    // The window ends when the last operation does, which is `until` or
    // later, so the rates, the object-store counters and the peak memory all
    // cover the same operations.
    let meters_after = ctx.meters.snapshot();
    let rss = rss.finish().await;
    let measured = Instant::now()
        .max(window.until)
        .duration_since(window.measure_from);

    let stats = std::mem::take(&mut *stats.lock().expect("the stats lock is not poisoned"));
    let (writes, reads) = stats
        .tenants
        .values()
        .fold((0, 0), |(w, r), t| (w + t.accepted_batches, r + t.queries));
    let object_store =
        meters_after.report_since(&meters_before, writes, reads, stats.maintenance.passes);
    let tenants: BTreeMap<&str, Value> = stats
        .tenants
        .iter()
        .map(|(tenant, t)| (*tenant, tenant_report(t, measured)))
        .collect();
    let primary = tenants.get(plan.primary).cloned().unwrap_or(Value::Null);
    let error_rate = stats.tenants.get(plan.primary).map_or(0.0, error_rate);
    let exercised = Exercised {
        writes: plan.writers.iter().any(|w| w.tenant == plan.primary),
        queries: plan.readers.iter().any(|r| r.tenant == plan.primary),
    };
    let objectives_met = meets(&primary, exercised, error_rate, &ctx.config.objectives);

    json!({
        "tenant": plan.primary,
        "warmup_seconds": plan.warmup.as_secs_f64(),
        "duration_seconds": measured.as_secs_f64(),
        "load": {
            "writers": plan.writers.iter().map(|w| json!({
                "tenant": w.tenant, "count": w.count, "series": w.series,
                "points_per_series": ctx.config.samples, "paced": w.paced,
            })).collect::<Vec<_>>(),
            "readers": plan.readers.iter().map(|r| json!({
                "tenant": r.tenant, "count": r.count,
                "window_seconds": r.window.as_secs(), "fresh": r.fresh,
                "interval_ms": u64::try_from(r.interval.as_millis()).unwrap_or(u64::MAX),
            })).collect::<Vec<_>>(),
            "maintenance": plan.maintenance.map(|m| match m {
                Maintenance::Compact => json!({"kind": "compact"}),
                Maintenance::Expire(secs) => json!({"kind": "expire", "retention_seconds": secs}),
            }),
        },
        "ingest": primary.get("ingest").cloned().unwrap_or(Value::Null),
        "query": primary.get("query").cloned().unwrap_or(Value::Null),
        "error_rate": error_rate,
        "objectives_met": objectives_met,
        "tenants": tenants,
        "rss_kib": rss,
        "wal_lag": wal_lag(),
        "object_store": object_store,
        "maintenance": maintenance_report(&stats.maintenance),
        "recovery_seconds": Value::Null,
        "rate_limiter": ctx.signal.rate_limiter(),
        "error_samples": stats.error_samples,
    })
}

async fn write_loop(ctx: Context, group: Writers, window: Window, stats: Arc<Mutex<Stats>>) {
    while window.open() {
        let batch = Batch {
            seq: ctx.seq.fetch_add(1, Ordering::Relaxed),
            series: group.series,
            samples: ctx.config.samples,
        };
        let started = Instant::now();
        let outcome = ctx.signal.write(group.tenant, batch).await;
        let elapsed = started.elapsed();
        if let Some(at) = window.offset(started + elapsed) {
            let mut stats = stats.lock().expect("the stats lock is not poisoned");
            let tenant = stats.tenant(group.tenant);
            match outcome {
                Ok(WriteOutcome::Accepted) => {
                    tenant.write_latency.observe(at, elapsed);
                    tenant.accepted_batches += 1;
                    tenant.accepted_rows += batch.rows();
                }
                Ok(WriteOutcome::Rejected) => {
                    tenant.rejected_batches += 1;
                    tenant.rejected_rows += batch.rows();
                }
                Err(message) => {
                    tenant.write_errors += 1;
                    stats.error(format!("write {}: {message}", group.tenant));
                }
            }
        }
        if group.paced {
            tokio::time::sleep(ctx.config.write_interval).await;
        } else {
            tokio::task::yield_now().await;
        }
    }
}

async fn read_loop(ctx: Context, group: Readers, window: Window, stats: Arc<Mutex<Stats>>) {
    while window.open() {
        let started = Instant::now();
        let outcome = ctx
            .signal
            .query(group.tenant, group.window, group.fresh)
            .await;
        let elapsed = started.elapsed();
        if let Some(at) = window.offset(started + elapsed) {
            let mut stats = stats.lock().expect("the stats lock is not poisoned");
            let tenant = stats.tenant(group.tenant);
            tenant.queries += 1;
            match outcome {
                Ok(rows) => {
                    tenant.query_latency.observe(at, elapsed);
                    tenant.rows_seen = rows;
                }
                Err(message) => {
                    tenant.query_errors += 1;
                    stats.error(format!("query {}: {message}", group.tenant));
                }
            }
        }
        if group.interval.is_zero() {
            tokio::task::yield_now().await;
        } else {
            tokio::time::sleep(group.interval).await;
        }
    }
}

async fn maintenance_loop(
    ctx: Context,
    maintenance: Maintenance,
    window: Window,
    stats: Arc<Mutex<Stats>>,
) {
    loop {
        tokio::time::sleep(ctx.config.maintenance_interval).await;
        if !window.open() {
            return;
        }
        let started = Instant::now();
        let outcome = match maintenance {
            Maintenance::Compact => ctx.signal.compact().await,
            Maintenance::Expire(secs) => ctx.signal.expire(secs).await,
        };
        let elapsed = started.elapsed();
        let Some(at) = window.offset(started + elapsed) else {
            continue;
        };
        let mut stats = stats.lock().expect("the stats lock is not poisoned");
        stats.maintenance.passes += 1;
        match outcome {
            Ok(report) => {
                stats.maintenance.latency.observe(at, elapsed);
                stats.maintenance.last = Some(report);
            }
            Err(message) => {
                stats.maintenance.errors += 1;
                stats.error(format!("maintenance: {message}"));
            }
        }
    }
}

fn tenant_report(t: &TenantStats, measured: Duration) -> Value {
    let seconds = measured.as_secs_f64().max(f64::MIN_POSITIVE);
    json!({
        "ingest": {
            "attempted_batches": t.accepted_batches + t.rejected_batches + t.write_errors,
            "accepted_batches": t.accepted_batches,
            "rejected_batches": t.rejected_batches,
            "errors": t.write_errors,
            "accepted_rows": t.accepted_rows,
            "rejected_rows": t.rejected_rows,
            "accepted_rows_per_sec": as_f64(t.accepted_rows) / seconds,
            "rejected_rows_per_sec": as_f64(t.rejected_rows) / seconds,
            "accepted_batches_per_sec": as_f64(t.accepted_batches) / seconds,
            "latency_us": t.write_latency.summary(),
        },
        "query": {
            "count": t.queries,
            "errors": t.query_errors,
            "per_sec": as_f64(t.queries) / seconds,
            "rows_seen_last": t.rows_seen,
            "latency_us": t.query_latency.summary(),
        },
        "error_rate": error_rate(t),
    })
}

/// Errors over attempts, writes and queries together. A batch the rate
/// limiter refused is an answer, not an error.
fn error_rate(t: &TenantStats) -> f64 {
    let attempts = t.accepted_batches + t.rejected_batches + t.write_errors + t.queries;
    ratio(t.write_errors + t.query_errors, attempts)
}

fn maintenance_report(m: &MaintenanceStats) -> Value {
    json!({
        "passes": m.passes,
        "errors": m.errors,
        "latency_us": m.latency.summary(),
        "last": m.last,
    })
}

/// Which paths a phase drove for its primary tenant.
#[derive(Clone, Copy, Debug)]
pub struct Exercised {
    pub writes: bool,
    pub queries: bool,
}

/// Whether one tenant's numbers meet the objectives.
///
/// A path the phase did not drive does not fail. A path it drove with no
/// successful operation has no p99, and that fails: a level that measured
/// nothing has not met an objective.
pub fn meets(
    tenant: &Value,
    exercised: Exercised,
    error_rate: f64,
    objectives: &Objectives,
) -> bool {
    let within = |driven: bool, path: &str, bound: Duration| {
        !driven
            || tenant
                .pointer(path)
                .and_then(Value::as_u64)
                .is_some_and(|p99| p99 <= duration_us(bound))
    };
    within(
        exercised.writes,
        "/ingest/latency_us/p99",
        objectives.write_p99,
    ) && within(
        exercised.queries,
        "/query/latency_us/p99",
        objectives.query_p99,
    ) && error_rate <= objectives.max_error_rate
}
