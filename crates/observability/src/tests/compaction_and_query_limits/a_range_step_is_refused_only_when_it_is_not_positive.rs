use super::*;

/// A range query's step is refused only when it is not positive; an absent
/// one falls back to the range's own default rather than to zero.
#[test]
pub(crate) fn a_range_step_is_refused_only_when_it_is_not_positive() {
    let range = TimeRange::new(0, 60_000_000_000).unwrap();
    for (name, step, expected) in [
        (
            "a positive step is kept as given",
            Some(1_000_000_i64),
            Some(1_000_000_i64),
        ),
        ("zero is refused", Some(0), None),
        ("a negative step is refused", Some(-1), None),
        (
            "an absent step defaults off the range",
            None,
            Some(default_metric_range_step(range)),
        ),
    ] {
        check!(resolved_range_step(step, range).ok() == expected, "{name}");
    }
    check!(default_metric_range_step(range) > 0);
}
