use super::*;

/// Merging two partial sample states keeps the smaller minimum, the larger
/// maximum, the earliest first and the latest last, taking each from
/// whichever side holds it. A tie on the timestamp keeps the side already
/// held -- the only thing that separates `<` from `<=` at either end.
#[test]
pub(crate) fn merging_sample_states_keeps_the_extremes_and_the_ends() {
    let value = |numerator: i128| super::super::prelude::MetricValue {
        numerator,
        denominator: 1,
    };

    let mut left = super::super::prelude::MetricSampleState {
        count: 1,
        min: Some(value(5)),
        max: Some(value(5)),
        first: Some((10, value(1))),
        last: Some((10, value(1))),
        ..Default::default()
    };
    // Every field of the incoming state wins: a lower minimum, a higher
    // maximum, an earlier first and a later last.
    left.merge(super::super::prelude::MetricSampleState {
        count: 1,
        min: Some(value(3)),
        max: Some(value(9)),
        first: Some((5, value(2))),
        last: Some((20, value(3))),
        ..Default::default()
    });

    check!(left.count == 2);
    check!(left.min == Some(value(3)), "the smaller minimum wins");
    check!(left.max == Some(value(9)), "the larger maximum wins");
    check!(left.first == Some((5, value(2))), "the earlier first wins");
    check!(left.last == Some((20, value(3))), "the later last wins");

    // Now the other way round, so neither side is simply preferred.
    let mut right = super::super::prelude::MetricSampleState {
        count: 1,
        min: Some(value(3)),
        max: Some(value(9)),
        first: Some((5, value(2))),
        last: Some((20, value(3))),
        ..Default::default()
    };
    right.merge(super::super::prelude::MetricSampleState {
        count: 1,
        min: Some(value(5)),
        max: Some(value(5)),
        first: Some((10, value(1))),
        last: Some((10, value(1))),
        ..Default::default()
    });
    check!(right.min == Some(value(3)), "the held minimum survives");
    check!(right.max == Some(value(9)), "the held maximum survives");
    check!(
        right.first == Some((5, value(2))),
        "the held first survives"
    );
    check!(right.last == Some((20, value(3))), "the held last survives");

    // Matching timestamps on both sides: the value already held stays.
    let mut tied = super::super::prelude::MetricSampleState {
        count: 1,
        first: Some((10, value(1))),
        last: Some((10, value(1))),
        ..Default::default()
    };
    tied.merge(super::super::prelude::MetricSampleState {
        count: 1,
        first: Some((10, value(7))),
        last: Some((10, value(7))),
        ..Default::default()
    });
    check!(
        tied.first == Some((10, value(1))),
        "a tie keeps the first already held"
    );
    check!(
        tied.last == Some((10, value(1))),
        "a tie keeps the last already held"
    );
}
