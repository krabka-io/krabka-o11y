//! CPU and allocation profiling of selective queries after index construction.
//!
//! Run with a series count, iteration count and matcher case. The cases are
//! `negative_first`, `broad_equality_first`, `negative_regex_first`, `broad_regex`
//! `logs_broad_equality_first` and `logs_broad_negative`.

use std::{hint::black_box, time::Instant};

use krabka_blockstore::{Index, LabelIndex, LabelMatcher, LabelPredicate, LogMatchOp, MatchOp};
use krabka_o11y_benches::index::{TENANT, populated_index, populated_log_label_index};

#[inline(never)]
fn resolve_query(index: &Index, matchers: &[LabelMatcher]) -> usize {
    black_box(
        black_box(index)
            .resolve(TENANT, black_box(matchers))
            .expect("the selector is valid"),
    )
    .len()
}

#[inline(never)]
fn resolve_log_query(index: &LabelIndex, predicates: &[LabelPredicate]) -> usize {
    black_box(black_box(index).match_series(TENANT, black_box(predicates))).len()
}

fn main() -> Result<(), &'static str> {
    let mut args = std::env::args().skip(1);
    let series = args
        .next()
        .expect("series count")
        .parse::<usize>()
        .expect("integer series count");
    let iterations = args
        .next()
        .expect("iteration count")
        .parse::<usize>()
        .expect("integer iteration count");
    let case = args.next().expect("matcher case");
    if matches!(
        case.as_str(),
        "logs_broad_equality_first" | "logs_broad_negative"
    ) {
        let index = populated_log_label_index(series);
        let second = if case == "logs_broad_negative" {
            LabelPredicate::new("job", LogMatchOp::NotEqual, "absent-job").unwrap()
        } else {
            LabelPredicate::new("pod", LogMatchOp::Equal, "pod-7").unwrap()
        };
        let predicates = [
            LabelPredicate::new("__name__", LogMatchOp::Equal, "http_requests_total").unwrap(),
            second,
        ];
        let started = Instant::now();
        let matched = (0..iterations)
            .map(|_| black_box(resolve_log_query(&index, &predicates)))
            .sum::<usize>();
        println!(
            "series={series} iterations={iterations} case={case} matched={matched} elapsed_seconds={}",
            started.elapsed().as_secs_f64(),
        );
        return Ok(());
    }
    let index = populated_index(series, 0);
    let (matchers, expected) = match case.as_str() {
        "broad_regex" => (
            [
                LabelMatcher::new("__name__", MatchOp::Eq, "http_requests_total"),
                LabelMatcher::new("job", MatchOp::Re, "job-(1|2|3)"),
            ],
            ["job-1", "job-2", "job-3"]
                .iter()
                .map(|job| {
                    index
                        .resolve(TENANT, &[LabelMatcher::new("job", MatchOp::Eq, *job)])
                        .expect("the equality selector is valid")
                        .len()
                })
                .sum(),
        ),
        "negative_first" | "broad_equality_first" | "negative_regex_first" => {
            let first = match case.as_str() {
                "negative_first" => LabelMatcher::new("job", MatchOp::Neq, "absent-job"),
                "broad_equality_first" => {
                    LabelMatcher::new("__name__", MatchOp::Eq, "http_requests_total")
                }
                _ => LabelMatcher::new("pod", MatchOp::Nre, "pod-(0|1|2|3)"),
            };
            ([first, LabelMatcher::new("pod", MatchOp::Eq, "pod-7")], 1)
        }
        _ => panic!("unknown matcher case"),
    };
    if resolve_query(&index, &matchers) != expected {
        return Err("the query must select the expected fixture series");
    }
    let started = Instant::now();
    let mut matched = 0;
    for _ in 0..iterations {
        matched += black_box(resolve_query(&index, &matchers));
    }
    println!(
        "series={series} iterations={iterations} case={case} matched={matched} elapsed_seconds={}",
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}
