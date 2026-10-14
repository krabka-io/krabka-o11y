use super::{ScalarSample, parse_decimal_sample_literal};

pub(crate) fn parse_scalar_sample(value: &str) -> Option<ScalarSample> {
    let ratio = parse_decimal_sample_literal(value)?;
    Some(ScalarSample::new(ratio.numerator, ratio.denominator))
}
