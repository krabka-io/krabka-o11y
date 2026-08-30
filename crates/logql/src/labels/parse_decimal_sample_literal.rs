use super::*;

pub(crate) fn parse_decimal_sample_literal(value: &str) -> Option<(i128, u128)> {
    if value.is_empty() {
        return None;
    }
    let (negative, value) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if value.starts_with('+') || value.starts_with('-') {
        return None;
    }
    if value.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match value.find(['e', 'E']) {
        Some(index) => {
            let exponent_text = &value[index + 1..];
            if exponent_text.find(['e', 'E']).is_some() {
                return None;
            }
            (&value[..index], parse_decimal_exponent(exponent_text)?)
        }
        None => (value, 0),
    };
    if mantissa.is_empty() {
        return None;
    }

    let (whole, fractional) = match mantissa.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (mantissa, ""),
    };
    if whole.is_empty() && fractional.is_empty() {
        return None;
    }
    let mut digits = String::with_capacity(whole.len() + fractional.len());
    digits.push_str(whole);
    digits.push_str(fractional);
    if digits.is_empty() {
        return None;
    }
    let mut numerator = digits.parse::<u128>().ok()?;

    let decimal_places = i64::try_from(fractional.len())
        .ok()?
        .checked_sub(i64::from(exponent))?;
    let denominator = if decimal_places >= 0 {
        10_u128.checked_pow(u32::try_from(decimal_places).ok()?)?
    } else {
        numerator =
            numerator.checked_mul(10_u128.checked_pow(u32::try_from(-decimal_places).ok()?)?)?;
        1
    };
    let numerator = i128::try_from(numerator).ok()?;
    Some((if negative { -numerator } else { numerator }, denominator))
}
