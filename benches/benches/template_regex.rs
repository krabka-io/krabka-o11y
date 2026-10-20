//! Pre-parsed regex templates over 100,000 log lines.

use std::{collections::BTreeMap, hint::black_box};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_logql::LineFormat;

fn template_regex(criterion: &mut Criterion) {
    let fields = BTreeMap::new();
    let line = "level=info method=GET path=/api/items status=200 duration=42ms";
    let mut group = criterion.benchmark_group("template_regex");
    group.sample_size(10);
    group.throughput(Throughput::Elements(100_000));
    for (pattern_name, pattern) in [
        ("simple", "info"),
        ("complex", r"(?P<key>[a-z]+)=(?P<value>[[:alnum:]/]+)"),
    ] {
        for function in ["count", "regexReplaceAll", "regexReplaceAllLiteral"] {
            let replacement = if function == "count" {
                ""
            } else {
                r#" "${key}:${value}""#
            };
            let format = LineFormat::new(format!(
                r#"{{{{ {function} `{pattern}` __line__{replacement} }}}}"#
            ))
            .unwrap();
            group.bench_function(BenchmarkId::new(function, pattern_name), |bencher| {
                bencher.iter(|| {
                    for _ in 0..100_000 {
                        black_box(format.render(black_box(line), &fields));
                    }
                });
            });
        }
    }
    group.finish();
}

criterion_group!(benches, template_regex);
criterion_main!(benches);
