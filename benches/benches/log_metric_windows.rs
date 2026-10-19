//! Complete log metric range queries across row counts and evaluation grids.

use std::hint::black_box;

use assert2::assert;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_o11y_benches::log_metric_windows::MetricWindowFixture;
use tokio::runtime::Runtime;

fn log_metric_windows(criterion: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let mut group = criterion.benchmark_group("log_metric_windows");
    group.sample_size(10);
    for rows in [10_000, 100_000, 1_000_000] {
        group.throughput(Throughput::Elements(u64::try_from(rows).unwrap()));
        for steps in [60, 360, 5_760] {
            let fixture = runtime.block_on(MetricWindowFixture::new(rows, steps));
            assert!(runtime.block_on(fixture.execute()) == fixture.expected);
            group.bench_function(
                BenchmarkId::new(format!("steps_{steps}"), rows),
                |bencher| {
                    bencher
                        .to_async(&runtime)
                        .iter(|| async { black_box(fixture.execute().await) });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, log_metric_windows);
criterion_main!(benches);
