use super::*;

/// Asserts the NaN-ignore rule for `min` and `max` on the `minmax_nan` queries.
///
/// The test checks absolute values. `min` and `max` take the extremum of the
/// mixed group over its non-NaN samples: min=1 and max=4. The engine keeps the
/// all-NaN group with a NaN result and does not drop the series.
pub(crate) fn assert_minmax_nan_ignoring(query: &str, via_operators: &[crate::InstantSample]) {
    // Look up a group's value by its `g` label.
    let by_group = |g: &str| -> f64 {
        let sample = via_operators
            .iter()
            .find(|sample| sample.labels.get("g") == Some(g))
            .unwrap_or_else(|| panic!("`{query}`: group g={g} missing"));
        float_value(&sample.value)
    };
    match query {
        "min by (g) (minmax_nan)" => {
            assert2::assert!(via_operators.len() == 2);
            let mixed = by_group("mixed");
            assert2::assert!(approx_eq(mixed, 1.0));
            let allnan = by_group("allnan");
            assert2::assert!(allnan.is_nan());
        }
        "max by (g) (minmax_nan)" => {
            assert2::assert!(via_operators.len() == 2);
            let mixed = by_group("mixed");
            assert2::assert!(approx_eq(mixed, 4.0));
            let allnan = by_group("allnan");
            assert2::assert!(allnan.is_nan());
        }
        // `min`/`max` with no grouping fold both groups together: the global
        // extremum is over the only non-NaN samples (the mixed group's
        // {4, 1}), so min=1 and max=4 (the all-NaN group is ignored, but its
        // presence does not force a NaN because the mixed group has finite
        // values).
        "min(minmax_nan)" => {
            assert2::assert!(via_operators.len() == 1);
            let value = float_value(&via_operators[0].value);
            assert2::assert!(approx_eq(value, 1.0));
        }
        "max(minmax_nan)" => {
            assert2::assert!(via_operators.len() == 1);
            let value = float_value(&via_operators[0].value);
            assert2::assert!(approx_eq(value, 4.0));
        }
        _ => {}
    }
}
