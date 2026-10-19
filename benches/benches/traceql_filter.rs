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
use krabka_traceql::{
    AttrValue, EngineOpts, MatchCmp, MatchScope, MatchValue, SpanMatcher, TraceqlEngine,
    attr_matches, parse,
};
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

    selector_regex(&mut group);
    group.finish();
}

/// Measures one selector scan, including preparation, against the previous per-value compilation.
fn selector_regex(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    for (label, value) in [
        ("scalar", AttrValue::Str("GET /api/traces".into())),
        (
            "array",
            AttrValue::Array(vec![
                AttrValue::Str("POST /api/traces".into()),
                AttrValue::Str("PUT /api/traces".into()),
                AttrValue::Str("GET /api/traces".into()),
            ]),
        ),
    ] {
        for spans in [100_u64, 1_000, 10_000] {
            group.throughput(Throughput::Elements(spans));
            for pattern in ["GET.*", "["] {
                let matchers = [SpanMatcher {
                    scope: MatchScope::Span,
                    key: "http.method".into(),
                    op: MatchCmp::Re,
                    value: MatchValue::Str(pattern.into()),
                    negated: false,
                }];
                for prepared in [false, true] {
                    let mode = if prepared { "prepared" } else { "per_value" };
                    let pattern_label = if pattern == "[" { "invalid" } else { "valid" };
                    group.bench_function(
                        BenchmarkId::new(
                            format!("selector_regex_{label}_{pattern_label}_{mode}"),
                            spans,
                        ),
                        |bencher| {
                            bencher.iter(|| {
                                let matchers = if prepared {
                                    SpanMatcher::prepare_regexes(&matchers)
                                } else {
                                    std::borrow::Cow::Borrowed(matchers.as_slice())
                                };
                                let matcher = &matchers[0];
                                for _ in 0..spans {
                                    black_box(attr_matches(
                                        black_box(&value),
                                        matcher.op,
                                        &matcher.value,
                                    ));
                                }
                            })
                        },
                    );
                }
            }
        }
    }
}

criterion_group!(benches, traceql_filter);
criterion_main!(benches);
