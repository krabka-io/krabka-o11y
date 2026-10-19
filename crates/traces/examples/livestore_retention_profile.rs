//! Compare indexed expiration with the former full scan using identical spans.
//!
//! Run: `cargo run -p krabka-traces --release --example livestore_retention_profile`
//! The optional first argument is the largest population (default 32768).
//! Timings include ingest and eviction, exclude fixture construction and teardown.
//! Query latency includes waiting for the same Tokio `RwLock` used by the WAL tail.

use std::{collections::BTreeMap, hint::black_box, sync::Arc, time::Instant};

use krabka_traces::{LiveStore, Span, SpanRecord};
use tokio::sync::{Barrier, RwLock};

type TenantSpans = BTreeMap<String, BTreeMap<[u8; 16], Vec<Span>>>;

// The old implementation is kept here as a measurement control, not a second
// production implementation. In particular, it scans on each frontier advance.
struct ScanStore {
    retention: i64,
    frontier: i64,
    tenants: TenantSpans,
}

trait Store: Send + Sync + 'static {
    fn new(retention: i64) -> Self;
    fn ingest(&mut self, record: SpanRecord);
    fn query(&self) -> Vec<Span>;
}

impl Store for ScanStore {
    fn new(retention: i64) -> Self {
        Self {
            retention,
            frontier: i64::MIN,
            tenants: BTreeMap::new(),
        }
    }

    fn ingest(&mut self, record: SpanRecord) {
        if record.span.start_ns > self.frontier {
            self.frontier = record.span.start_ns;
            let cutoff = self.frontier.saturating_sub(self.retention);
            self.tenants.retain(|_, traces| {
                traces.retain(|_, spans| {
                    spans.retain(|span| span.start_ns >= cutoff);
                    !spans.is_empty()
                });
                !traces.is_empty()
            });
        }
        if record.span.start_ns >= self.frontier.saturating_sub(self.retention) {
            self.tenants
                .entry(record.tenant)
                .or_default()
                .entry(record.span.trace_id)
                .or_default()
                .push(record.span);
        }
    }

    fn query(&self) -> Vec<Span> {
        let mut spans = self
            .tenants
            .get("tenant-0")
            .and_then(|traces| traces.get(&[0; 16]))
            .cloned()
            .unwrap_or_default();
        spans.sort_by_key(|span| (span.start_ns, span.span_id));
        spans
    }
}

impl Store for LiveStore {
    fn new(retention: i64) -> Self {
        Self::new(retention)
    }
    fn ingest(&mut self, record: SpanRecord) {
        self.ingest(record);
    }
    fn query(&self) -> Vec<Span> {
        self.trace_by_id("tenant-0", &[0; 16])
    }
}

fn records(start: usize, count: usize, one_trace: bool) -> Vec<SpanRecord> {
    (start..start + count)
        .map(|index| SpanRecord {
            tenant: format!("tenant-{}", if one_trace { 0 } else { index % 4 }),
            span: Span {
                trace_id: if one_trace {
                    [0; 16]
                } else {
                    u128::try_from(index % 256).unwrap().to_be_bytes()
                },
                span_id: u64::try_from(index).unwrap().to_be_bytes(),
                start_ns: i64::try_from(index).unwrap(),
                duration_ns: 1,
                name: "retention-profile".into(),
                ..Span::default()
            },
        })
        .collect()
}

fn filled<S: Store>(population: usize, one_trace: bool) -> S {
    let mut store = S::new(i64::try_from(population).unwrap());
    for record in records(0, population, one_trace) {
        store.ingest(record);
    }
    store
}

fn ingest_profile<S: Store>(label: &str, population: usize, one_trace: bool) {
    let mut store = S::new(i64::try_from(population).unwrap());
    let inputs = records(0, population, one_trace);
    let started = Instant::now();
    for record in inputs {
        store.ingest(record);
    }
    let fill = started.elapsed();
    let inputs = records(population, population, one_trace);
    let started = Instant::now();
    for record in inputs {
        store.ingest(record);
    }
    let steady = started.elapsed();
    black_box(&store);
    println!(
        "{label},retained={population},one_trace={one_trace},fill_ns_per_span={:.0},steady_ns_per_span={:.0}",
        fill.as_secs_f64() * 1e9 / f64::from(u32::try_from(population).unwrap()),
        steady.as_secs_f64() * 1e9 / f64::from(u32::try_from(population).unwrap())
    );
}

async fn query_profile<S: Store>(label: &str, population: usize) {
    // Many traces keep the queried trace small, so latency exposes lock blocking.
    let store = Arc::new(RwLock::new(filled::<S>(population, false)));
    let barrier = Arc::new(Barrier::new(2));
    let inputs = records(population, 4096, false);
    let writer_store = Arc::clone(&store);
    let writer_barrier = Arc::clone(&barrier);
    let writer = tokio::spawn(async move {
        writer_barrier.wait().await;
        let mut inputs = inputs.into_iter();
        loop {
            let mut guard = writer_store.write().await;
            let mut count = 0;
            for record in inputs.by_ref().take(64) {
                guard.ingest(record);
                count += 1;
            }
            drop(guard);
            if count == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    });
    barrier.wait().await;
    let mut latencies = Vec::with_capacity(1024);
    loop {
        let started = Instant::now();
        black_box(store.read().await.query());
        latencies.push(started.elapsed().as_nanos());
        if writer.is_finished() {
            break;
        }
        tokio::task::yield_now().await;
    }
    writer.await.unwrap();
    latencies.sort_unstable();
    let count = latencies.len();
    println!(
        "{label},retained={population},queries={count},query_ns_p50={},query_ns_p95={},query_ns_p99={},query_ns_max={}",
        latencies[count / 2],
        latencies[count * 95 / 100],
        latencies[count * 99 / 100],
        latencies[count - 1]
    );
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let largest = std::env::args()
        .nth(1)
        .map_or(32768, |arg| arg.parse::<usize>().unwrap());
    assert!(largest >= 16);
    for population in [largest / 16, largest / 4, largest] {
        for one_trace in [false, true] {
            ingest_profile::<ScanStore>("scan", population, one_trace);
            ingest_profile::<LiveStore>("indexed", population, one_trace);
        }
        query_profile::<ScanStore>("scan", population).await;
        query_profile::<LiveStore>("indexed", population).await;
    }
}
