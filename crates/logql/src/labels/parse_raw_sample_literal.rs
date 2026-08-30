use super::*;

pub(crate) fn parse_raw_sample_literal(value: &str) -> Option<String> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    let negative = numerator < 0;
    let formatted = format_decimal_ratio(numerator.unsigned_abs(), denominator);
    Some(if negative {
        format!("-{formatted}")
    } else {
        formatted
    })
}
