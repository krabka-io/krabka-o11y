use super::*;

/// Checks a `fill_right(0)` group match of `http_requests_total` instances
/// `a` = 100, `b` = 50 (job `api`) and `c` = 7 (job `worker`) against a `job="api"`
/// one side of 10 with `region="east"`: the matched instances gain the region
/// and the 10, and the unmatched `c` keeps its value without a region.
pub(crate) fn assert_filled_many_side(samples: &[crate::InstantSample]) {
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("instance").expect("instance label"),
                (sample.labels.get("region"), float_value(&sample.value)),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert2::assert!(values.len() == 3);
    assert2::assert!(values["a"].0 == Some("east"));
    assert2::assert!(approx_eq(values["a"].1, 110.0));
    assert2::assert!(values["b"].0 == Some("east"));
    assert2::assert!(approx_eq(values["b"].1, 60.0));
    assert2::assert!(values["c"].0 == None);
    assert2::assert!(approx_eq(values["c"].1, 7.0));
}
