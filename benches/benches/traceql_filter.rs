//! `TraceQL` span filtering, end to end through the engine.
//!
//! `crates/traceql` is one of the two crates with the least test mass per
//! source line, and it has never been run above a handful of spans. The sweep
//! here is span count, and the cases are chosen to separate the two things a
//! filter does: an attribute predicate, which a columnar scan should answer in
//! one pass, and a structural operator, which has to relate spans to each
//! other and is where a quadratic implementation would hide.

use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_o11y_benches::{
    index::TENANT,
    spans::{START_NS, end_ns, span_store},
};
use krabka_traceql::{EngineOpts, TraceqlEngine, parse};
use tokio::runtime::Runtime;

/// The queries under test.
const QUERIES: [(&str, &str); 4] = [
    ("attr_eq", r#"{ .service.name = "svc-3" }"#),
    ("intrinsic_duration", "{ span:duration > 500us }"),
    (
        "conjunction",
        r#"{ .service.name = "svc-3" && .http.method = "GET" }"#,
    ),
    (
        "descendant",
        r#"{ .service.name = "svc-3" } >> { .http.status_code = 500 }"#,
    ),
];

/// Traces in the store, with spans per trace held at ten. The largest is a
/// hundred thousand spans; the scale suite carries the million-span case,
/// because a million spans in a Criterion fixture is a minute of setup before
/// the first measurement.
const TRACES: [usize; 3] = [100, 1_000, 10_000];
const SPANS_PER_TRACE: usize = 10;

/// The limit a search is given. Twenty is the engine's own default and what a
/// Grafana panel sends.
const LIMIT: usize = 20;

fn traceql_filter(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let mut group = criterion.benchmark_group("traceql_filter");
    group.sample_size(10);

    for (label, query) in QUERIES {
        group.bench_function(BenchmarkId::new("parse", label), |bencher| {
            bencher.iter(|| {
                let parsed = parse(query).expect("the fixture query parses");
                black_box(parsed)
            });
        });
    }

    for traces in TRACES {
        let spans = traces * SPANS_PER_TRACE;
        let engine = TraceqlEngine::new(
            Arc::new(span_store(traces, SPANS_PER_TRACE)),
            EngineOpts::default(),
        );
        let end = end_ns(SPANS_PER_TRACE);

        group.throughput(Throughput::Elements(
            u64::try_from(spans).expect("a span count fits a u64"),
        ));
        for (label, query) in QUERIES {
            group.bench_with_input(BenchmarkId::new(label, spans), &spans, |bencher, _| {
                bencher.to_async(&runtime).iter(|| async {
                    let response = engine
                        .search(TENANT, query, START_NS, end, LIMIT)
                        .await
                        .expect("the fixture query evaluates");
                    black_box(response)
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, traceql_filter);
criterion_main!(benches);
