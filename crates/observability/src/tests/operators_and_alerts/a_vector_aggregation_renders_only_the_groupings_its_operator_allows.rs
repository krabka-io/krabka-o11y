use super::*;

/// `format_vector_aggregation_query` renders an aggregation back to its
/// `LogQL` spelling. Most operators take an optional grouping clause, but
/// three -- `approx_topk`, sort and `sort_desc` -- have no grouped form and
/// must refuse rather than render one, so each is checked BOTH ways.
///
/// The two limit-taking operators put their limit inside the parentheses
/// ahead of the inner query, where the ungrouped ones do not, which is why
/// the names alone are not enough to pin them.
#[test]
pub(crate) fn a_vector_aggregation_renders_only_the_groupings_its_operator_allows() {
    use krabka_logql::{VectorAggregation, VectorAggregationOp, VectorGrouping};

    let render = |op, grouping| {
        super::super::prelude::format_vector_aggregation_query(
            &VectorAggregation { op, grouping },
            "up",
        )
    };
    let by = || {
        Some(VectorGrouping::By(vec![
            "job".to_string(),
            "app".to_string(),
        ]))
    };
    let without = || Some(VectorGrouping::Without(vec!["pod".to_string()]));

    // Plain operators, ungrouped and grouped both ways.
    check!(render(VectorAggregationOp::Sum, None) == Some("sum(up)".to_string()));
    check!(render(VectorAggregationOp::Count, None) == Some("count(up)".to_string()));
    check!(render(VectorAggregationOp::Min, None) == Some("min(up)".to_string()));
    check!(render(VectorAggregationOp::Max, None) == Some("max(up)".to_string()));
    check!(render(VectorAggregationOp::Avg, None) == Some("avg(up)".to_string()));
    check!(render(VectorAggregationOp::Stddev, None) == Some("stddev(up)".to_string()));
    check!(render(VectorAggregationOp::Stdvar, None) == Some("stdvar(up)".to_string()));

    // The grouping is joined with a comma and sits before the parentheses.
    check!(render(VectorAggregationOp::Sum, by()) == Some("sum by (job,app)(up)".to_string()));
    check!(
        render(VectorAggregationOp::Max, without()) == Some("max without (pod)(up)".to_string())
    );

    // The limit-taking operators put their limit inside, before the inner
    // query, and still accept a grouping.
    check!(render(VectorAggregationOp::TopK(3), None) == Some("topk(3,up)".to_string()));
    check!(render(VectorAggregationOp::BottomK(3), None) == Some("bottomk(3,up)".to_string()));
    check!(
        render(VectorAggregationOp::TopK(5), by()) == Some("topk by (job,app)(5,up)".to_string())
    );

    // These three have no grouped form: rendered ungrouped, refused with
    // a grouping. Both directions matter -- a mutant that dropped the
    // guard would render an expression LogQL cannot parse back.
    check!(render(VectorAggregationOp::Sort, None) == Some("sort(up)".to_string()));
    check!(render(VectorAggregationOp::Sort, by()).is_none());
    check!(render(VectorAggregationOp::SortDesc, None) == Some("sort_desc(up)".to_string()));
    check!(render(VectorAggregationOp::SortDesc, without()).is_none());
    check!(
        render(VectorAggregationOp::ApproxTopK(4), None) == Some("approx_topk(4,up)".to_string())
    );
    check!(render(VectorAggregationOp::ApproxTopK(4), by()).is_none());

    // count_values has no rendering at all, grouped or not.
    check!(render(VectorAggregationOp::CountValues("x".to_string()), None).is_none());
    check!(render(VectorAggregationOp::CountValues("x".to_string()), by()).is_none());
}
