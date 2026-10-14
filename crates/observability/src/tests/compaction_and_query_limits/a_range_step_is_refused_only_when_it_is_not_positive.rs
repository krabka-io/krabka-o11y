use super::*;

struct RangeStepCase {
    name: &'static str,
    step: Option<i64>,
    expected: Option<i64>,
}

/// A range query's step is refused only when it is not positive; an absent
/// one falls back to the range's own default rather than to zero.
#[test]
pub(crate) fn a_range_step_is_refused_only_when_it_is_not_positive() {
    let range = TimeRange::new(0, 60_000_000_000).unwrap();
    for case in [
        RangeStepCase {
            name: "a positive step is kept as given",
            step: Some(1_000_000_i64),
            expected: Some(1_000_000_i64),
        },
        RangeStepCase {
            name: "zero is refused",
            step: Some(0),
            expected: None,
        },
        RangeStepCase {
            name: "a negative step is refused",
            step: Some(-1),
            expected: None,
        },
        RangeStepCase {
            name: "an absent step defaults off the range",
            step: None,
            expected: Some(default_metric_range_step(range)),
        },
    ] {
        check!(
            resolved_range_step(case.step, range).ok() == case.expected,
            "{}",
            case.name
        );
    }
    check!(default_metric_range_step(range) > 0);
}
