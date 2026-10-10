//! Selective multi-matcher queries over large logs label indexes.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use krabka_blockstore::{LabelPredicate, LogMatchOp};
use krabka_o11y_benches::index::{TENANT, populated_log_label_index};

const SERIES: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

fn log_index_matchers(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("log_index_matchers");
    group.sample_size(10);
    for series in SERIES {
        let index = populated_log_label_index(series);
        let selected = LabelPredicate::new("pod", LogMatchOp::Equal, "pod-7").unwrap();
        let broad =
            LabelPredicate::new("__name__", LogMatchOp::Equal, "http_requests_total").unwrap();
        let negative = LabelPredicate::new("job", LogMatchOp::NotEqual, "absent-job").unwrap();
        for (name, predicates) in [
            ("equality", vec![selected.clone()]),
            (
                "broad_equality_first",
                vec![broad.clone(), selected.clone()],
            ),
            ("broad_equality_last", vec![selected.clone(), broad]),
            ("negative_first", vec![negative.clone(), selected.clone()]),
            ("negative_last", vec![selected, negative]),
            (
                "broad_negative",
                vec![
                    LabelPredicate::new("__name__", LogMatchOp::Equal, "http_requests_total")
                        .unwrap(),
                    LabelPredicate::new("job", LogMatchOp::NotEqual, "absent-job").unwrap(),
                ],
            ),
            (
                "two_broad_equalities",
                vec![
                    LabelPredicate::new("__name__", LogMatchOp::Equal, "http_requests_total")
                        .unwrap(),
                    LabelPredicate::new("job", LogMatchOp::Equal, "job-3").unwrap(),
                ],
            ),
        ] {
            group.bench_with_input(
                BenchmarkId::new(name, series),
                &predicates,
                |bencher, predicates| {
                    bencher.iter(|| black_box(index.match_series(TENANT, black_box(predicates))));
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, log_index_matchers);
criterion_main!(benches);
