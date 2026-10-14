use super::*;

/// Checks a `fill_right(0)` group match of `http_requests_total` instances
/// `a` = 100, `b` = 50 (job `api`) and `c` = 7 (job `worker`) against a `job="api"`
/// one side of 10 with `region="east"`: the matched instances gain the region
/// and the 10, and the unmatched `c` keeps its value without a region.
pub(crate) fn assert_filled_many_side(samples: &[crate::InstantSample]) {
    let mut values = BTreeMap::new();
    for sample in samples {
        values.insert(
            sample.labels.get("instance").expect("instance label"),
            FilledSideValue {
                region: sample.labels.get("region"),
                value: float_value(&sample.value),
            },
        );
    }
    assert2::assert!(values.len() == 3);
    assert2::assert!(values["a"].region == Some("east"));
    assert2::assert!(approx_eq(values["a"].value, 110.0));
    assert2::assert!(values["b"].region == Some("east"));
    assert2::assert!(approx_eq(values["b"].value, 60.0));
    assert2::assert!(values["c"].region == None);
    assert2::assert!(approx_eq(values["c"].value, 7.0));
}

struct FilledSideValue<'a> {
    region: Option<&'a str>,
    value: f64,
}
