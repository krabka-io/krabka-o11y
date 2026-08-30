use super::*;

/// Asserts the sparse aggregate-over-rate rule for the `sparse_total` queries.
///
/// The test checks absolute values. The engine excludes the sparse no-value
/// series from its group. The `g="mix"` group survives with only the
/// contribution of its dense member. The result has no row for the
/// all-no-value `g="allsparse"` group: the series is absent, not present with
/// NaN.
pub(crate) fn assert_sparse_aggregate_excludes_no_value(query: &str, via_operators: &[crate::InstantSample]) {
    let group_value = |g: &str| -> Option<f64> {
        via_operators
            .iter()
            .find(|sample| sample.labels.get("g") == Some(g))
            .map(|sample| float_value(&sample.value))
    };
    match query {
        // g="mix" survives (its dense member has a rate); g="allsparse" has
        // no value-bearing member, so it is absent. Only one row total.
        "sum by (g) (rate(sparse_total[2m]))"
        | "avg by (g) (rate(sparse_total[2m]))"
        | "min by (g) (rate(sparse_total[2m]))"
        | "max by (g) (rate(sparse_total[2m]))"
        | "count by (g) (rate(sparse_total[2m]))"
        | "group by (g) (rate(sparse_total[2m]))" => {
            assert2::assert!(via_operators.len() == 1);
            assert2::assert!(group_value("mix").is_some());
            assert2::assert!(group_value("allsparse").is_none());
            // `count`/`group` over g=mix see exactly the one dense member.
            if query == "count by (g) (rate(sparse_total[2m]))" {
                assert2::assert!(approx_eq(group_value("mix").unwrap(), 1.0));
            }
            if query == "group by (g) (rate(sparse_total[2m]))" {
                assert2::assert!(approx_eq(group_value("mix").unwrap(), 1.0));
            }
        }
        // No grouping: the global aggregate is over the single dense rate.
        "count (rate(sparse_total[2m]))" => {
            assert2::assert!(via_operators.len() == 1);
            assert2::assert!(approx_eq(float_value(&via_operators[0].value), 1.0));
        }
        "sum (rate(sparse_total[2m]))" => {
            assert2::assert!(via_operators.len() == 1);
        }
        // The `*_over_time` window strands every sparse member, leaving only
        // the dense member in g=mix; g=allsparse is absent.
        "count by (g) (avg_over_time(sparse_total[30s]))" => {
            assert2::assert!(via_operators.len() == 1);
            assert2::assert!(approx_eq(group_value("mix").unwrap(), 1.0));
            assert2::assert!(group_value("allsparse").is_none());
        }
        _ => {}
    }
}
