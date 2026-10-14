use super::*;

struct AggregationRecognitionCase {
    query: &'static str,
    count_values: bool,
    approx_topk: bool,
}

/// `count_values` and `approx_topk` are both refused before a query is
/// planned. The two predicates read different aggregation fields, so a
/// query using one must not trip the other.
#[yare::parameterized(
    count_values = { AggregationRecognitionCase { query: r#"count_values("status", rate({job="api"}[1m]))"#, count_values: true, approx_topk: false } },
    approx_topk = { AggregationRecognitionCase { query: r#"approx_topk(3, rate({job="api"}[1m]))"#, count_values: false, approx_topk: true } },
    sum = { AggregationRecognitionCase { query: r#"sum(rate({job="api"}[1m]))"#, count_values: false, approx_topk: false } },
    rate = { AggregationRecognitionCase { query: r#"rate({job="api"}[1m])"#, count_values: false, approx_topk: false } },
)]
pub(crate) fn count_values_and_approx_topk_are_recognised_apart_from_each_other(
    case: AggregationRecognitionCase,
) {
    let parsed = parse_metric_query(case.query).expect(case.query);
    check!(
        metric_query_uses_count_values(&parsed) == case.count_values,
        "{}",
        case.query
    );
    check!(
        metric_query_uses_approx_topk(&parsed) == case.approx_topk,
        "{}",
        case.query
    );
}
