use super::*;

/// `has_samples` gates every aggregate that would otherwise divide by a
/// count of zero, so it must be false at zero and true at one.
#[test]
pub(crate) fn a_sample_state_has_samples_from_the_first_one() {
    let mut state = super::super::prelude::MetricSampleState::default();
    check!(!state.has_samples(), "an empty state has none");

    state.count = 1;
    check!(state.has_samples(), "one sample is enough");
    state.count = 100;
    check!(state.has_samples());
}
