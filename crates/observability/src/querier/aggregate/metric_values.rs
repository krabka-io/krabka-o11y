use num_traits::{FromPrimitive as _, ToPrimitive};

pub(crate) use super::record_matching::append_matching_log_row;
use crate::{
    BTreeMap, METRIC_DECIMAL_SCALE, MetricValue, Ordering, Quantile, QueryError, Rational,
    TimeRange, VectorAggregationOp, gcd_signed, impl_rational_division_ops,
    impl_rational_float_conversion,
};

mod eval_times;
mod metric_sample_state;
mod metric_value;
mod vector_aggregation_state;

pub(crate) use eval_times::{checked_eval_times, eval_times};
pub(crate) use metric_sample_state::MetricSampleState;
pub(crate) use vector_aggregation_state::VectorAggregationState;

pub(crate) fn format_metric_value(value: MetricValue) -> String {
    let negative = value.numerator < 0;
    let numerator = value.numerator.unsigned_abs();
    let whole = numerator / value.denominator;
    let mut remainder = numerator % value.denominator;
    let sign = if negative { "-" } else { "" };
    if remainder == 0 {
        return format!("{sign}{whole}");
    }

    let mut decimals = String::new();
    while remainder != 0 && decimals.len() < 9 {
        remainder *= 10;
        let digit =
            u8::try_from(remainder / value.denominator).expect("decimal digit is less than 10");
        decimals.push(char::from(b'0' + digit));
        remainder %= value.denominator;
    }
    while decimals.ends_with('0') {
        decimals.pop();
    }
    format!("{sign}{whole}.{decimals}")
}

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
