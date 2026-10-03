//! The eight phases, in the order one signal runs them.
//!
//! The order is part of the shape. The cold-block phase reads what the three
//! phases before it wrote. The deletion phase comes after it, because
//! deletion removes those blocks. The restart phase comes last, because it
//! recovers from everything the others left behind.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use serde_json::{Value, json};

use super::{
    SignalKind, Stores,
    rss::RssSampler,
    runner::{self, Context, Maintenance, Plan, Readers, Writers, wal_lag},
};

/// Every phase name, in run order. The gate checks a report against this.
pub const PHASES: [&str; 8] = [
    "steady",
    "burst",
    "high_cardinality",
    "cold_blocks",
    "compaction",
    "deletion",
    "noisy_tenant",
    "restart",
];

const TENANT: &str = "soak";
const QUIET: &str = "quiet";
const NOISY: &str = "noisy";

/// Runs every phase for one signal and returns one entry per phase.
pub async fn run_signal(kind: SignalKind, ctx: &Context, stores: &Stores) -> Vec<Value> {
    let mut entries = Vec::new();
    let mut push = |phase: &str, mut entry: Value| {
        entry["signal"] = json!(kind.name());
        entry["phase"] = json!(phase);
        eprintln!(
            "soak: {} {phase}: objectives_met={} error_rate={}",
            kind.name(),
            entry["objectives_met"],
            entry["error_rate"],
        );
        entries.push(entry);
    };
    push("steady", runner::run(ctx, &steady(ctx, TENANT)).await);
    push("burst", burst(ctx).await);
    push("high_cardinality", high_cardinality(ctx).await);
    push("cold_blocks", runner::run(ctx, &cold_blocks(ctx)).await);
    push(
        "compaction",
        runner::run(ctx, &during(ctx, Maintenance::Compact)).await,
    );
    let retention = ctx.config.retention_secs;
    push(
        "deletion",
        runner::run(ctx, &during(ctx, Maintenance::Expire(retention))).await,
    );
    push("noisy_tenant", noisy_tenant(ctx).await);
    push("restart", restart(kind, ctx, stores).await);
    entries
}

fn full(ctx: &Context) -> (Duration, Duration) {
    (ctx.config.warmup, ctx.config.phase)
}

/// A load level inside a stepped phase runs for half a phase, so a four-step
/// search costs two phases, not four.
fn step(ctx: &Context) -> (Duration, Duration) {
    (ctx.config.warmup / 2, ctx.config.phase / 2)
}

fn paced(tenant: &'static str, count: u32, series: u32) -> Writers {
    Writers {
        tenant,
        count,
        series,
        paced: true,
    }
}

fn readers(ctx: &Context, tenant: &'static str, count: u32) -> Readers {
    Readers {
        tenant,
        count,
        window: ctx.config.query_window,
        fresh: false,
        interval: Duration::from_millis(250),
    }
}

/// Two paced writers and two readers on one tenant.
fn steady(ctx: &Context, tenant: &'static str) -> Plan {
    let (warmup, duration) = full(ctx);
    Plan {
        writers: vec![paced(tenant, 2, ctx.config.series)],
        readers: vec![readers(ctx, tenant, 2)],
        maintenance: None,
        primary: tenant,
        warmup,
        duration,
    }
}

/// The steady load with a maintenance pass running beside it.
fn during(ctx: &Context, maintenance: Maintenance) -> Plan {
    Plan {
        maintenance: Some(maintenance),
        ..steady(ctx, TENANT)
    }
}

/// No writes. Every query reloads the index, builds a new engine, and reads
/// every block the earlier phases wrote.
fn cold_blocks(ctx: &Context) -> Plan {
    let (warmup, duration) = full(ctx);
    Plan {
        writers: Vec::new(),
        readers: vec![Readers {
            tenant: TENANT,
            count: 1,
            window: ctx.config.full_window,
            fresh: true,
            interval: Duration::ZERO,
        }],
        maintenance: None,
        primary: TENANT,
        warmup,
        duration,
    }
}

/// Steps unpaced writers up until a level misses an objective, and reports
/// the highest level that met them all.
async fn burst(ctx: &Context) -> Value {
    let (warmup, duration) = step(ctx);
    let mut levels = Vec::new();
    for &level in &ctx.config.burst_levels {
        let plan = Plan {
            writers: vec![Writers {
                tenant: TENANT,
                count: level,
                series: ctx.config.series,
                paced: false,
            }],
            readers: vec![Readers {
                interval: Duration::from_millis(500),
                ..readers(ctx, TENANT, 1)
            }],
            maintenance: None,
            primary: TENANT,
            warmup,
            duration,
        };
        let entry = runner::run(ctx, &plan).await;
        let met = entry["objectives_met"].as_bool().unwrap_or(false);
        levels.push((level, entry));
        if !met {
            break;
        }
    }
    stepped(levels, "writers")
}

/// Steps the series per batch up, with the steady writers and readers.
async fn high_cardinality(ctx: &Context) -> Value {
    let (warmup, duration) = step(ctx);
    let mut levels = Vec::new();
    for &series in &ctx.config.cardinality_steps {
        let plan = Plan {
            writers: vec![paced(TENANT, 1, series)],
            warmup,
            duration,
            ..steady(ctx, TENANT)
        };
        let entry = runner::run(ctx, &plan).await;
        let met = entry["objectives_met"].as_bool().unwrap_or(false);
        levels.push((series, entry));
        if !met {
            break;
        }
    }
    stepped(levels, "series_per_batch")
}

/// Folds a stepped search into one entry. The top-level numbers are the
/// highest level that met every objective, or the first level when none did,
/// and `levels` keeps every step.
fn stepped(levels: Vec<(u32, Value)>, unit: &str) -> Value {
    let saturated = levels
        .iter()
        .rev()
        .find(|(_, entry)| entry["objectives_met"].as_bool().unwrap_or(false));
    let limited = levels
        .iter()
        .find(|(_, entry)| !entry["objectives_met"].as_bool().unwrap_or(false))
        .map(|(level, _)| *level);
    let mut entry = saturated
        .or(levels.first())
        .map_or(Value::Null, |(_, entry)| entry.clone());
    entry["saturation"] = json!({
        "unit": unit,
        "level": saturated.map(|(level, _)| *level),
        "accepted_rows_per_sec": saturated.map(|(_, e)| e["ingest"]["accepted_rows_per_sec"].clone()),
        "first_failing_level": limited,
        "status": match (saturated, limited) {
            (Some(_), Some(_)) => "saturated",
            (Some(_), None) => "not_reached",
            (None, _) => "below_first_level",
        },
    });
    entry["levels"] = levels
        .into_iter()
        .map(|(level, e)| {
            json!({
                "level": level,
                "objectives_met": e["objectives_met"],
                "error_rate": e["error_rate"],
                "accepted_rows_per_sec": e["ingest"]["accepted_rows_per_sec"],
                "write_p99_us": e["ingest"]["latency_us"]["p99"],
                "query_p99_us": e["query"]["latency_us"]["p99"],
                "rss_kib": e["rss_kib"],
                "object_store": e["object_store"],
            })
        })
        .collect();
    entry
}

/// A quiet tenant at the steady rate beside a noisy one writing unpaced into
/// its rate limit. The entry's numbers are the quiet tenant's.
async fn noisy_tenant(ctx: &Context) -> Value {
    let (warmup, duration) = full(ctx);
    let rows = ctx.config.noisy_rows_per_sec;
    ctx.signal.limit(NOISY, rows, u64::from(rows));
    let plan = Plan {
        writers: vec![
            paced(QUIET, 2, ctx.config.series),
            Writers {
                tenant: NOISY,
                count: ctx.config.noisy_writers,
                series: ctx.config.series,
                paced: false,
            },
        ],
        readers: vec![readers(ctx, QUIET, 2), readers(ctx, NOISY, 2)],
        maintenance: None,
        primary: QUIET,
        warmup,
        duration,
    };
    let mut entry = runner::run(ctx, &plan).await;
    entry["noisy_limit_rows_per_sec"] = json!(rows);
    entry
}

/// Opens the signal again over the same stores, as a restarted process does,
/// and times it to the first query that sees data.
async fn restart(kind: SignalKind, ctx: &Context, stores: &Stores) -> Value {
    let before = ctx.meters.snapshot();
    let rss = RssSampler::start();
    let started = Instant::now();
    let mut attempts = 0_u64;
    let mut errors = Vec::new();
    let mut recovered = None;
    let mut rows = 0;
    while started.elapsed() < ctx.config.recovery_timeout {
        attempts += 1;
        let outcome = match kind.open(stores.clone(), ctx.config.seed).await {
            Ok(signal) => signal.query(TENANT, ctx.config.full_window, true).await,
            Err(message) => Err(message),
        };
        match outcome {
            Ok(seen) if seen > 0 => {
                rows = seen;
                recovered = Some(started.elapsed());
                break;
            }
            Ok(_) => {}
            Err(message) => errors.push(message),
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let after = ctx.meters.snapshot();
    let rss = rss.finish().await;
    let error_count = u64::try_from(errors.len()).unwrap_or(u64::MAX);
    json!({
        "tenant": TENANT,
        "warmup_seconds": 0.0,
        "duration_seconds": started.elapsed().as_secs_f64(),
        "recovery_seconds": recovered.map(|elapsed| elapsed.as_secs_f64()),
        "recovery": {
            "status": if recovered.is_some() { "recovered" } else { "timed_out" },
            "attempts": attempts,
            "rows_seen": rows,
            "timeout_seconds": ctx.config.recovery_timeout.as_secs(),
            "measures": "a cold open of the signal over the same object store, then queries \
                         until the first one returns data written before the restart",
        },
        "objectives_met": recovered.is_some() && errors.is_empty(),
        "error_rate": super::latency::ratio(error_count, attempts),
        "errors": error_count,
        "rss_kib": rss,
        "wal_lag": wal_lag(),
        "object_store": after.report_since(&before, 0, attempts, 0),
        "rate_limiter": ctx.signal.rate_limiter(),
        "error_samples": errors.into_iter().take(5).collect::<Vec<_>>(),
    })
}

/// Builds the context for one signal: its stores' counters, and its first
/// open.
pub async fn context(
    kind: SignalKind,
    stores: &Stores,
    meters: super::store_stats::StoreMeters,
    config: &Arc<super::config::Config>,
) -> Context {
    let signal = kind
        .open(stores.clone(), config.seed)
        .await
        .unwrap_or_else(|message| panic!("{} opens over an empty store: {message}", kind.name()));
    Context {
        signal,
        meters,
        seq: Arc::new(std::sync::atomic::AtomicI64::new(1)),
        config: Arc::clone(config),
    }
}
