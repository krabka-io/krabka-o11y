//! HTTP preparation, index cache hits, shard execution and response serialization.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use krabka_o11y_benches::log_frontend::{CASES, LogFrontendFixture};
use tokio::runtime::Runtime;

fn log_query_frontend(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let mut group = criterion.benchmark_group("log_query_frontend");
    group.sample_size(10);
    for streams in [1_000, 5_000, 20_000, 100_000] {
        let fixture = runtime.block_on(LogFrontendFixture::new(streams));
        for name in CASES {
            group.bench_function(BenchmarkId::new(name, streams), |bencher| {
                runtime
                    .block_on(fixture.verify(name))
                    .expect("the complete payload matches");
                bencher.to_async(&runtime).iter(|| async {
                    black_box(fixture.execute(name).await.expect("the request succeeds"))
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, log_query_frontend);
criterion_main!(benches);
