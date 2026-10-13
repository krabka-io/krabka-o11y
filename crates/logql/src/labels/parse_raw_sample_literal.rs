use super::{format_decimal_ratio, parse_decimal_sample_literal};

pub(crate) fn parse_raw_sample_literal(value: &str) -> Option<String> {
    let ratio = parse_decimal_sample_literal(value)?;
    let numerator = ratio.numerator;
    let negative = numerator < 0;
    let formatted = format_decimal_ratio(numerator.unsigned_abs(), ratio.denominator);
    Some(if negative {
        format!("-{formatted}")
    } else {
        formatted
    })
}
