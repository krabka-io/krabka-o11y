use super::*;

pub(crate) fn parse_bytes_literal(value: &str) -> Option<ByteSize> {
    let unit_start = value
        .find(|ch: char| ch.is_ascii_alphabetic())
        .unwrap_or(value.len());
    let amount = value[..unit_start].parse::<f64>().ok()?;
    if !amount.is_finite() || amount < 0.0 {
        return None;
    }
    let multiplier = bytes_unit_multiplier(&value[unit_start..])?;
    Some(ByteSize::from_bytes_f64(amount * multiplier))
}
