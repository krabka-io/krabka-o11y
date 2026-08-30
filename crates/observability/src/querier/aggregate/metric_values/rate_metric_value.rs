use super::*;

pub(crate) fn rate_metric_value(value: MetricValue, range_ns: i64) -> MetricValue {
    let denominator = u128::from(range_ns.unsigned_abs());
    if denominator == 0 {
        return MetricValue::zero();
    }

    MetricValue::new(
        value.numerator * 1_000_000_000,
        value.denominator * denominator,
    )
}
