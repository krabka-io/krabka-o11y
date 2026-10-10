//! Selective multi-matcher queries over large tenant indexes.
//!
//! Most cases return one series. Broad controls also measure selectors that
//! retain many series. Matchers expose work that grows with tenant size.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use krabka_blockstore::{LabelMatcher, MatchOp};
use krabka_o11y_benches::index::{TENANT, populated_index};

const SERIES: [usize; 4] = [1_000, 10_000, 100_000, 1_000_000];

fn index_matchers(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("index_matchers");
    group.sample_size(10);

    for series in SERIES {
        let index = populated_index(series, 0);
        let selected = LabelMatcher::new("pod", MatchOp::Eq, "pod-7");
        let negative = LabelMatcher::new("job", MatchOp::Neq, "absent-job");
        let broad = LabelMatcher::new("__name__", MatchOp::Eq, "http_requests_total");
        let cases = [
            ("equality", vec![selected.clone()]),
            ("negative_first", vec![negative.clone(), selected.clone()]),
            ("negative_last", vec![selected.clone(), negative]),
            (
                "broad_equality_first",
                vec![broad.clone(), selected.clone()],
            ),
            ("broad_equality_last", vec![selected.clone(), broad]),
            (
                "positive_regex_first",
                vec![
                    LabelMatcher::new("pod", MatchOp::Re, ".+"),
                    selected.clone(),
                ],
            ),
            (
                "negative_regex_first",
                vec![
                    LabelMatcher::new("pod", MatchOp::Nre, "pod-(0|1|2|3)"),
                    selected.clone(),
                ],
            ),
            (
                "absent_label_first",
                vec![LabelMatcher::new("zone", MatchOp::Eq, ""), selected.clone()],
            ),
            (
                "shard_first",
                vec![
                    LabelMatcher::new("__query_shard__", MatchOp::Eq, "1_of_1"),
                    selected,
                ],
            ),
            (
                "broad_negative",
                vec![
                    LabelMatcher::new("__name__", MatchOp::Eq, "http_requests_total"),
                    LabelMatcher::new("job", MatchOp::Neq, "absent-job"),
                ],
            ),
            (
                "broad_regex",
                vec![
                    LabelMatcher::new("__name__", MatchOp::Eq, "http_requests_total"),
                    LabelMatcher::new("job", MatchOp::Re, "job-(1|2|3)"),
                ],
            ),
            (
                "job_regex",
                vec![
                    LabelMatcher::new("job", MatchOp::Eq, "job-3"),
                    LabelMatcher::new("instance", MatchOp::Re, "instance-(1|2|3)"),
                ],
            ),
        ];
        for (name, matchers) in cases {
            group.bench_with_input(
                BenchmarkId::new(name, series),
                &matchers,
                |bencher, matchers| {
                    bencher.iter(|| {
                        black_box(
                            index
                                .resolve(TENANT, black_box(matchers))
                                .expect("the selector is valid"),
                        )
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(benches, index_matchers);
criterion_main!(benches);
