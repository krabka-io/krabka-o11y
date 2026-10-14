use super::*;

#[bon::builder]
fn tied_sample_state(value: i128) -> super::super::prelude::MetricSampleState {
    let value = super::super::prelude::MetricValue {
        numerator: value,
        denominator: 1,
    };
    super::super::prelude::MetricSampleState {
        summary: super::super::prelude::VectorAggregationState {
            count: 1,
            ..Default::default()
        },
        first: Some(super::super::prelude::TimestampedMetricValue {
            timestamp_ns: 10,
            value,
        }),
        last: Some(super::super::prelude::TimestampedMetricValue {
            timestamp_ns: 10,
            value,
        }),
        ..Default::default()
    }
}

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

    // One side holds a single 5 at 10 ns, the other a 3 to 9 spread from
    // 5 ns to 20 ns.
    let narrow = super::super::prelude::MetricSampleState {
        summary: super::super::prelude::VectorAggregationState {
            count: 1,
            min: Some(value(5)),
            max: Some(value(5)),
            ..Default::default()
        },
        first: Some(super::super::prelude::TimestampedMetricValue {
            timestamp_ns: 10,
            value: value(1),
        }),
        last: Some(super::super::prelude::TimestampedMetricValue {
            timestamp_ns: 10,
            value: value(1),
        }),
        ..Default::default()
    };
    let wide = super::super::prelude::MetricSampleState {
        summary: super::super::prelude::VectorAggregationState {
            count: 1,
            min: Some(value(3)),
            max: Some(value(9)),
            ..Default::default()
        },
        first: Some(super::super::prelude::TimestampedMetricValue {
            timestamp_ns: 5,
            value: value(2),
        }),
        last: Some(super::super::prelude::TimestampedMetricValue {
            timestamp_ns: 20,
            value: value(3),
        }),
        ..Default::default()
    };

    // Every field of the incoming state wins: a lower minimum, a higher
    // maximum, an earlier first and a later last.
    let mut left = narrow.clone();
    left.merge(wide.clone());

    check!(left.summary.count == 2);
    check!(
        left.summary.min == Some(value(3)),
        "the smaller minimum wins"
    );
    check!(
        left.summary.max == Some(value(9)),
        "the larger maximum wins"
    );
    check!(
        left.first
            == Some(super::super::prelude::TimestampedMetricValue {
                timestamp_ns: 5,
                value: value(2)
            }),
        "the earlier first wins"
    );
    check!(
        left.last
            == Some(super::super::prelude::TimestampedMetricValue {
                timestamp_ns: 20,
                value: value(3)
            }),
        "the later last wins"
    );

    // Now the other way round, so neither side is simply preferred.
    let mut right = wide;
    right.merge(narrow);
    check!(
        right.summary.min == Some(value(3)),
        "the held minimum survives"
    );
    check!(
        right.summary.max == Some(value(9)),
        "the held maximum survives"
    );
    check!(
        right.first
            == Some(super::super::prelude::TimestampedMetricValue {
                timestamp_ns: 5,
                value: value(2)
            }),
        "the held first survives"
    );
    check!(
        right.last
            == Some(super::super::prelude::TimestampedMetricValue {
                timestamp_ns: 20,
                value: value(3)
            }),
        "the held last survives"
    );

    // Matching timestamps on both sides: the value already held stays.
    let mut tied = tied_sample_state().value(1).call();
    tied.merge(tied_sample_state().value(7).call());
    check!(
        tied.first
            == Some(super::super::prelude::TimestampedMetricValue {
                timestamp_ns: 10,
                value: value(1)
            }),
        "a tie keeps the first already held"
    );
    check!(
        tied.last
            == Some(super::super::prelude::TimestampedMetricValue {
                timestamp_ns: 10,
                value: value(1)
            }),
        "a tie keeps the last already held"
    );
}
