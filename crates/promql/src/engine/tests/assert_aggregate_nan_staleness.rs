use super::*;

/// Asserts the staleness semantics for the `nan_metric` parity queries.
///
/// `sum` keeps the genuine NaN as a NaN value, not as a stale marker. `count`
/// drops the stale-NaN marker before it counts, so the result is 2, not 3.
pub(crate) fn assert_aggregate_nan_staleness(query: &str, via_operators: &[crate::InstantSample]) {
    if query == "sum(nan_metric)" {
        assert2::assert!(via_operators.len() == 1);
        let value = float_value(&via_operators[0].value);
        assert2::assert!(value.is_nan());
        assert2::assert!(!super::is_stale_nan(value));
    }
    if query == "count(nan_metric)" {
        assert2::assert!(via_operators.len() == 1);
        let value = float_value(&via_operators[0].value);
        assert2::assert!(approx_eq(value, 2.0));
    }
}
