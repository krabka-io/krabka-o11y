//! Complete Parquet-backed Loki responses with large resolved fingerprint sets.

use std::{hint::black_box, sync::Arc};

use assert2::assert;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_o11y_benches::log_queries::{CASES, LogQueryFixture, POINTS_PER_STREAM};
use krabka_observability::execute_stream_query_from_object_store;
use tokio::runtime::Runtime;

fn log_stream_query(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let mut group = criterion.benchmark_group("log_stream_query");
    group.sample_size(10);
    for streams in [1_000, 5_000, 20_000, 100_000] {
        let fixture = runtime.block_on(LogQueryFixture::new(streams));
        group.throughput(Throughput::Elements(
            u64::try_from(streams * POINTS_PER_STREAM).expect("a count fits u64"),
        ));
        for name in CASES {
            group.bench_function(BenchmarkId::new(name, streams), |bencher| {
                let case = fixture.case(name);
                let actual = runtime
                    .block_on(execute_stream_query_from_object_store(
                        Arc::clone(&fixture.store),
                        &fixture.prefix,
                        &case.plan,
                        &fixture.label_index,
                    ))
                    .expect("the query executes");
                assert!(
                    actual == case.expected,
                    "the complete response matches the input ledger"
                );
                bencher.to_async(&runtime).iter(|| async {
                    black_box(
                        execute_stream_query_from_object_store(
                            Arc::clone(&fixture.store),
                            &fixture.prefix,
                            &case.plan,
                            &fixture.label_index,
                        )
                        .await
                        .expect("the query executes"),
                    )
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, log_stream_query);
criterion_main!(benches);
