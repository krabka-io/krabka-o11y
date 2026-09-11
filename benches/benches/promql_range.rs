//! `PromQL` range evaluation, end to end through the engine.
//!
//! `query_range` plans and executes once per step, so its cost is steps times
//! series and the two are swept separately. The parse case underneath is the
//! floor: it is the same query through `parse_promql` alone, so the gap
//! between it and the evaluation cases says how much of a range query is
//! planning rather than data, which is the number that decides whether a plan
//! cache would be worth having.

#![recursion_limit = "512"]

use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_blockstore::TenantId;
use krabka_o11y_benches::{
    index::TENANT,
    metrics::{SCRAPE_INTERVAL_MS, metric_store},
};
use krabka_promql::{EngineOpts, PromqlEngine, parse_promql};
use krabka_units::prelude::*;
use tokio::runtime::Runtime;

/// The queries under test. A bare selector, a rate over a window, and an
/// aggregation over that rate -- which is what a dashboard panel actually
/// sends, and the one whose cost rises with series rather than with steps.
const QUERIES: [(&str, &str); 3] = [
    ("selector", "http_requests_total"),
    ("rate", "rate(http_requests_total[5m])"),
    ("sum_by_job", "sum by (job) (rate(http_requests_total[5m]))"),
];

/// Series counts to sweep, with samples and steps held fixed.
const SERIES: [usize; 3] = [100, 1_000, 10_000];

/// Samples per series in the store. Two hundred at a fifteen-second scrape is
/// fifty minutes, enough for a five-minute `rate` window to be full at every
/// step the benchmark evaluates.
const SAMPLES: usize = 200;

/// Steps a range query evaluates. Sixty is an hour at a one-minute step, which
/// is a dashboard's default range and the number a panel actually asks for.
const STEPS: i64 = 60;

fn promql_range(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let mut group = criterion.benchmark_group("promql_range");
    // Each iteration is `STEPS` DataFusion plans and executions, so one sample
    // is already tens of milliseconds at the smaller sizes and seconds at the
    // larger ones.
    group.sample_size(10);

    let step = millis(60_000);
    let end_ms = SCRAPE_INTERVAL_MS * i64::try_from(SAMPLES).expect("a sample count fits an i64");
    let start_ms = end_ms - STEPS * 60_000;

    for (label, query) in QUERIES {
        // Parsing alone, at no series count: it does not read the store, so it
        // is one point rather than a sweep.
        group.bench_function(BenchmarkId::new("parse", label), |bencher| {
            bencher.iter(|| {
                let expr = parse_promql(query).expect("the fixture query parses");
                black_box(expr)
            });
        });
    }

    // The engine takes the validated tenant id, so the benchmark builds one
    // from the fixture name once rather than per iteration.
    let tenant = TenantId::new(TENANT).expect("the benchmark tenant is a valid tenant id");

    for series in SERIES {
        let engine = PromqlEngine::new(
            Arc::new(metric_store(series, SAMPLES)),
            EngineOpts::default(),
        );
        group.throughput(Throughput::Elements(
            u64::try_from(series).expect("a series count fits a u64"),
        ));

        for (label, query) in QUERIES {
            group.bench_with_input(BenchmarkId::new(label, series), &series, |bencher, _| {
                bencher.to_async(&runtime).iter(|| async {
                    let result = engine
                        .query_range(&tenant, query, start_ms, end_ms, step)
                        .await
                        .expect("the fixture query evaluates");
                    black_box(result)
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, promql_range);
criterion_main!(benches);
