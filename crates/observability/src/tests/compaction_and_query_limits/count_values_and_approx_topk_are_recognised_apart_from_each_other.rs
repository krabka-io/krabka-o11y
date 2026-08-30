use super::*;

/// `count_values` and `approx_topk` are both refused before a query is
/// planned. The two predicates read different aggregation fields, so a
/// query using one must not trip the other.
#[test]
pub(crate) fn count_values_and_approx_topk_are_recognised_apart_from_each_other() {
    for (query, count_values, approx_topk) in [
        (
            r#"count_values("status", rate({job="api"}[1m]))"#,
            true,
            false,
        ),
        (r#"approx_topk(3, rate({job="api"}[1m]))"#, false, true),
        (r#"sum(rate({job="api"}[1m]))"#, false, false),
        (r#"rate({job="api"}[1m])"#, false, false),
    ] {
        let parsed = parse_metric_query(query).expect(query);
        check!(
            metric_query_uses_count_values(&parsed) == count_values,
            "{query}"
        );
        check!(
            metric_query_uses_approx_topk(&parsed) == approx_topk,
            "{query}"
        );
    }
}
