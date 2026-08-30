use super::*;

/// Recording keeps the earliest sample and the latest, and a later record
/// at a timestamp already held changes neither. The four below arrive out
/// of order and revisit both ends: without the revisits, the guards could
/// take the last writer at each end instead of the first.
#[test]
pub(crate) fn recording_samples_keeps_the_earliest_and_the_latest() {
    let value = |numerator: i128| super::super::prelude::MetricValue {
        numerator,
        denominator: 1,
    };
    let mut state = super::super::prelude::MetricSampleState::default();

    state.record(10, value(1));
    state.record(5, value(2));
    // Neither of these displaces an end: one repeats the latest timestamp,
    // the other the earliest.
    state.record(10, value(3));
    state.record(5, value(4));

    check!(state.count == 4);
    check!(
        state.first == Some((5, value(2))),
        "the earliest timestamp, from the first record that reached it"
    );
    check!(
        state.last == Some((10, value(1))),
        "the latest timestamp, from the first record that reached it"
    );
}
