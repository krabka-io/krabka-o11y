use super::parse_decimal_exponent;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecimalSampleRatio {
    pub numerator: i128,
    pub denominator: u128,
}

struct SignedDigits<'a> {
    negative: bool,
    digits: &'a str,
}

struct DecimalMantissa<'a> {
    digits: &'a str,
    exponent: i32,
}

struct WholeAndFraction<'a> {
    whole: &'a str,
    fractional: &'a str,
}

/// Parses a decimal sample literal, such as `-1.25e3`, into an exact
/// `(numerator, denominator)` ratio.
///
/// The literal is an optional sign, ASCII digits with at most one `.`, and an
/// optional `e`/`E` exponent. Returns `None` for anything else, or when the
/// ratio overflows `i128`/`u128`.
#[must_use]
pub fn parse_decimal_sample_literal(value: &str) -> Option<DecimalSampleRatio> {
    if value.is_empty() {
        return None;
    }
    let signed_digits = match value.as_bytes().first() {
        Some(b'-') => SignedDigits {
            negative: true,
            digits: &value[1..],
        },
        Some(b'+') => SignedDigits {
            negative: false,
            digits: &value[1..],
        },
        _ => SignedDigits {
            negative: false,
            digits: value,
        },
    };
    let value = signed_digits.digits;
    let negative = signed_digits.negative;
    if value.is_empty() {
        return None;
    }

    let decimal_mantissa = match value.find(['e', 'E']) {
        Some(index) => {
            let exponent_text = &value[index + 1..];
            if exponent_text.find(['e', 'E']).is_some() {
                return None;
            }
            DecimalMantissa {
                digits: &value[..index],
                exponent: parse_decimal_exponent(exponent_text)?,
            }
        }
        None => DecimalMantissa {
            digits: value,
            exponent: 0,
        },
    };
    let mantissa = decimal_mantissa.digits;
    let exponent = decimal_mantissa.exponent;
    if mantissa.is_empty() {
        return None;
    }

    let whole_and_fraction = match mantissa.split_once('.') {
        Some(parts) => WholeAndFraction {
            whole: parts.0,
            fractional: parts.1,
        },
        None => WholeAndFraction {
            whole: mantissa,
            fractional: "",
        },
    };
    let whole = whole_and_fraction.whole;
    let fractional = whole_and_fraction.fractional;
    if whole.is_empty() && fractional.is_empty() {
        return None;
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
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
    Some(DecimalSampleRatio {
        numerator: if negative { -numerator } else { numerator },
        denominator,
    })
}
