//! Complete profile queries with repeated stack IDs and trace selections.

use std::{hint::black_box, sync::Arc};

use assert2::assert;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_o11y_benches::{
    index::TENANT,
    profile_samples::{PROFILE_TYPE, profile_samples},
};
use krabka_pprof::{EngineOpts, FlameEngine, SampleSelector};
use tokio::runtime::Runtime;

fn profile_query(criterion: &mut Criterion) {
    let runtime = Runtime::new().expect("a tokio runtime");
    let mut group = criterion.benchmark_group("profile_query");
    group.sample_size(10);
    for samples in [1_000, 10_000, 100_000, 1_000_000] {
        let fixture = profile_samples(samples, 64);
        let engine = FlameEngine::new(Arc::new(fixture.store), EngineOpts::default());
        let call_sites = ["main".to_string()];
        let cases = [
            ("grouped", SampleSelector::None, &[][..], &fixture.all),
            (
                "trace_all",
                SampleSelector::Trace(&fixture.traces),
                &[][..],
                &fixture.all,
            ),
            (
                "trace_one",
                SampleSelector::Trace(&fixture.traces[..1]),
                &[][..],
                &fixture.one_trace,
            ),
            (
                "trace_callsite",
                SampleSelector::Trace(&fixture.traces),
                &call_sites[..],
                &fixture.main,
            ),
        ];
        group.throughput(Throughput::Elements(
            u64::try_from(samples).expect("a count fits u64"),
        ));
        for (case, selector, sites, expected) in cases {
            let actual = runtime
                .block_on(engine.select_merge_stacktraces_with_selectors(
                    (TENANT, PROFILE_TYPE, "{service=\"api\"}"),
                    (0, fixture.end_ms),
                    i64::MAX,
                    sites,
                    selector,
                ))
                .expect("the fixture query evaluates");
            assert!(
                actual == *expected,
                "the query must match the complete fixture ledger"
            );
            group.bench_function(BenchmarkId::new(case, samples), |bencher| {
                bencher.to_async(&runtime).iter(|| async {
                    black_box(
                        engine
                            .select_merge_stacktraces_with_selectors(
                                (TENANT, PROFILE_TYPE, "{service=\"api\"}"),
                                (0, fixture.end_ms),
                                i64::MAX,
                                sites,
                                selector,
                            )
                            .await
                            .expect("the fixture query evaluates"),
                    )
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, profile_query);
criterion_main!(benches);
