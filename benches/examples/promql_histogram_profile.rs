//! Native-histogram rate queries with independent duration, series and bucket sweeps.

use std::{hint::black_box, sync::Arc, time::Instant};

use assert2::assert;
use krabka_blockstore::{Labels, TenantId};
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};
use krabka_promql::{EngineOpts, InMemoryMetricStore, PromqlEngine, QueryResult, SampleValue};
use krabka_units::prelude::*;

const TENANT: &str = "histogram-benchmark";
const QUERY: &str = "rate(request_duration_seconds[5m])";
const SCRAPE_MS: i64 = 15_000;
const START_MS: i64 = 300_000;

#[derive(Clone, Copy)]
struct Case {
    duration_minutes: u32,
    series: u32,
    buckets: u32,
}

fn engine(case: Case) -> PromqlEngine<InMemoryMetricStore> {
    let mut store = InMemoryMetricStore::new();
    let end_ms = START_MS + i64::from(case.duration_minutes) * 60_000;
    for series in 0..case.series {
        let mut labels = Labels::new();
        labels.insert("__name__", "request_duration_seconds");
        labels.insert("instance", series.to_string());
        for sample in 0..=end_ms / SCRAPE_MS {
            let count = f64::from(u32::try_from(sample + 1).unwrap());
            store.push_histogram(
                TENANT,
                labels.clone(),
                sample * SCRAPE_MS,
                NativeHistogram {
                    schema: 0,
                    is_float: true,
                    reset_hint: ResetHint::No,
                    zero_threshold: 0.0,
                    zero_count: 0.0,
                    count: count * f64::from(case.buckets),
                    sum: count * f64::from(case.buckets),
                    positive_spans: vec![BucketSpan {
                        offset: 0,
                        length: case.buckets,
                    }],
                    positive_counts: vec![count; usize::try_from(case.buckets).unwrap()],
                    negative_spans: vec![],
                    negative_counts: vec![],
                    custom_values: None,
                    start_timestamp_ms: None,
                },
            );
        }
    }
    PromqlEngine::new(Arc::new(store), EngineOpts::default())
}

async fn query(
    engine: &PromqlEngine<InMemoryMetricStore>,
    tenant: &TenantId,
    case: Case,
) -> QueryResult {
    engine
        .query_range(
            tenant,
            QUERY,
            START_MS,
            START_MS + i64::from(case.duration_minutes) * 60_000,
            millis(60_000),
        )
        .await
        .expect("the fixture query evaluates")
}

fn verify(result: QueryResult, case: Case) {
    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected a matrix");
    };
    assert!(series.len() == usize::try_from(case.series).unwrap());
    for series in series {
        assert!(series.samples.len() == usize::try_from(case.duration_minutes + 1).unwrap());
        for sample in series.samples {
            let SampleValue::Histogram(histogram) = sample.value else {
                panic!("expected a histogram rate");
            };
            assert!((histogram.count - f64::from(case.buckets) / 15.0).abs() < 1e-9);
            assert!(histogram.positive_counts.len() == usize::try_from(case.buckets).unwrap());
            assert!(
                histogram
                    .positive_counts
                    .iter()
                    .all(|count| (*count - 1.0 / 15.0).abs() < 1e-9)
            );
        }
    }
}

fn main() {
    let iterations = std::env::args()
        .nth(1)
        .map_or(3, |value| value.parse::<u32>().unwrap());
    assert!(iterations > 0);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let tenant = TenantId::new(TENANT).unwrap();
    let base = Case {
        duration_minutes: 30,
        series: 10,
        buckets: 32,
    };
    let cases = [
        base,
        Case {
            duration_minutes: 5,
            ..base
        },
        Case {
            duration_minutes: 120,
            ..base
        },
        Case { series: 1, ..base },
        Case {
            series: 100,
            ..base
        },
        Case { buckets: 4, ..base },
        Case {
            buckets: 256,
            ..base
        },
    ];
    println!("duration_minutes,series,buckets,mean_us,allocations,allocated_bytes,peak_bytes");
    for case in cases {
        let engine = engine(case);
        allocation_counter::opt_out(|| {
            verify(runtime.block_on(query(&engine, &tenant, case)), case)
        });
        let started = Instant::now();
        allocation_counter::opt_out(|| {
            for _ in 0..iterations {
                black_box(runtime.block_on(query(&engine, &tenant, case)));
            }
        });
        let mean_us = started.elapsed().as_secs_f64() * 1e6 / f64::from(iterations);
        let allocations = allocation_counter::measure(|| {
            black_box(runtime.block_on(query(&engine, &tenant, case)));
        });
        println!(
            "{},{},{},{mean_us:.2},{},{},{}",
            case.duration_minutes,
            case.series,
            case.buckets,
            allocations.count_total,
            allocations.bytes_total,
            allocations.bytes_max,
        );
    }
}
