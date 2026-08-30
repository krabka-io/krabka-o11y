use super::*;

pub(crate) fn parse_metric_sample_value(value: &str) -> Option<MetricValue> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    Some(MetricValue::new(numerator, denominator))
}
