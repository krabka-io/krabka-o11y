//! Applying metrics WAL records to the hot head while a query reads it.
//!
//! The querier's hot head is a copy-on-write `Arc<InMemoryMetricStore>` behind
//! a lock: a query snapshots it by cloning the pointer and holds that snapshot
//! for the whole scan, and a write calls `Arc::make_mut`, which copies the
//! store whenever a snapshot is outstanding. So the cost of ingest depends on
//! something ingest cannot see -- whether anyone is querying -- and the three
//! cases below are the ones that decide it.
//!
//! `per_record_with_reader` is the worst case and the one the WAL tail used to
//! produce: a query starts between every pair of records, so every record finds
//! the store shared and copies it. `per_batch_with_reader` is what the tail does
//! now: a poll's records go in under one `make_mut`, so a batch copies once
//! however long it is. `per_batch_no_reader` is the case that must not
//! regress -- nothing is shared, `make_mut` takes its fast path, and the work is
//! the appends alone.
//!
//! The sweep is over series count, because that is what the copy is linear in.
//! A curve that stays flat across it is the claim being tested: that a write's
//! cost has stopped scaling with the size of the head.
//!
//! The COW wrapper is spelled out here rather than driven through `WalHead`, so
//! that all three cases are the same few lines with one thing changed between
//! them, and so that a reader snapshot can be held across an exact number of
//! writes. `WalHead` is that wrapper plus a lock, and the lock is uncontended in
//! all three.

use std::{
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_o11y_benches::metrics::{metric_store, wal_records};
use krabka_promql::InMemoryMetricStore;

/// Series counts to sweep. Thousands of series is a small production tenant and
/// the size at which a full-store copy stops being free.
const SERIES: [usize; 2] = [1_000, 5_000];

/// Samples per series already in the head when the batch arrives. Twenty at a
/// fifteen-second scrape is five minutes of head.
const SAMPLES: usize = 20;

/// Records in a batch. A real poll returns more, which only widens the gap
/// between the per-record and per-batch cases, so a short batch is the
/// conservative number to report.
const BATCH: usize = 16;

/// Iterations the clone-free case runs before it rebuilds its store.
///
/// That case is the only one whose store survives an iteration, since nothing
/// copies it, so it grows by `BATCH` rows each time. Rebuilding periodically --
/// outside the timed region -- keeps the head near the size the sweep asked for
/// instead of letting it drift upwards across the run.
const RESET_EVERY: u64 = 2_048;

fn wal_head_apply(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("wal_head_apply");
    // One iteration of the shared cases is `BATCH` full-store copies, which is
    // already tens of milliseconds at the larger size.
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(2));

    let batch = wal_records(BATCH);
    group.throughput(Throughput::Elements(
        u64::try_from(BATCH).expect("a batch size fits a u64"),
    ));

    for series in SERIES {
        let template = Arc::new(metric_store(series, SAMPLES));

        // A query starts between every pair of records, so every record finds
        // the head shared and copies it. The snapshot is dropped as soon as the
        // write lands, which is what a finishing query does, and freeing the
        // copy is part of what the copy costs.
        group.bench_with_input(
            BenchmarkId::new("per_record_with_reader", series),
            &series,
            |bencher, _| {
                bencher.iter_batched(
                    || Arc::clone(&template),
                    |mut store| {
                        for record in &batch {
                            let snapshot = Arc::clone(&store);
                            Arc::make_mut(&mut store).apply_wal_record(record);
                            drop(black_box(snapshot));
                        }
                        store
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        // One query is running, and the whole batch goes in under the one
        // `make_mut` its snapshot forces.
        group.bench_with_input(
            BenchmarkId::new("per_batch_with_reader", series),
            &series,
            |bencher, _| {
                bencher.iter_batched(
                    || Arc::clone(&template),
                    |mut store| {
                        let snapshot = Arc::clone(&store);
                        let head = Arc::make_mut(&mut store);
                        for record in &batch {
                            head.apply_wal_record(record);
                        }
                        drop(black_box(snapshot));
                        store
                    },
                    BatchSize::PerIteration,
                );
            },
        );

        // Nobody is querying: `make_mut` copies nothing and the batch is the
        // appends alone. This is the case that must not have got slower.
        group.bench_with_input(
            BenchmarkId::new("per_batch_no_reader", series),
            &series,
            |bencher, _| {
                bencher.iter_custom(|iters| {
                    let mut store = Arc::new(template.as_ref().clone());
                    let mut elapsed = Duration::ZERO;
                    for iteration in 0..iters {
                        if iteration > 0 && iteration % RESET_EVERY == 0 {
                            store = Arc::new(template.as_ref().clone());
                        }
                        let started = Instant::now();
                        let head: &mut InMemoryMetricStore = Arc::make_mut(&mut store);
                        for record in &batch {
                            head.apply_wal_record(record);
                        }
                        elapsed += started.elapsed();
                        black_box(&store);
                    }
                    elapsed
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, wal_head_apply);
criterion_main!(benches);
