use super::{MetricValue, parse_decimal_sample_literal};

pub(crate) fn parse_metric_sample_value(value: &str) -> Option<MetricValue> {
    let ratio = parse_decimal_sample_literal(value)?;
    Some(MetricValue::new(ratio.numerator, ratio.denominator))
}
