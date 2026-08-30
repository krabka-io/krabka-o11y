use super::*;

/// `MetricValue` is a rational scaled by a fixed decimal factor, so a
/// float arriving from a metric has to survive the round trip through that
/// scale, and the values it cannot represent have to be refused rather
/// than rounded into something plausible.
#[test]
pub(crate) fn metric_values_round_trip_through_their_decimal_scale() {
    use super::super::prelude::MetricValue;

    let round_trip =
        |value: f64| MetricValue::from_f64(value).and_then(super::super::prelude::MetricValue::to_f64);

    check!(round_trip(0.0) == Some(0.0));
    check!(round_trip(1.0) == Some(1.0));
    check!(round_trip(-1.0) == Some(-1.0));
    check!(round_trip(0.5) == Some(0.5));
    check!(round_trip(-2.25) == Some(-2.25));
    check!(round_trip(1234.5) == Some(1234.5));

    // The scale is a billion, so a nanosecond-sized fraction survives and
    // anything finer rounds to the nearest step rather than to zero.
    check!(round_trip(0.000_000_001) == Some(0.000_000_001));
    check!(
        round_trip(0.000_000_000_4) == Some(0.0),
        "below half a step rounds down"
    );
    check!(
        round_trip(0.000_000_000_6) == Some(0.000_000_001),
        "above half rounds up"
    );

    // Values that are not numbers cannot be represented at all.
    check!(MetricValue::from_f64(f64::NAN) == None);
    check!(MetricValue::from_f64(f64::INFINITY) == None);
    check!(MetricValue::from_f64(f64::NEG_INFINITY) == None);
}
