//! Go ideal-number syntax and strconv.ParseFloat, including hexadecimal floats.
//! Primary Go1.26.5 text/template/parse/node.go and internal/strconv/atof.go.
//! Conversion independently rounds the retained hexadecimal significand to
//! nearest-even, preserving a sticky bit for arbitrarily long literals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Literal {
    Signed(i64),
    Unsigned(u64),
    Float(u64),
    Complex(u64, u64),
}

pub(super) fn literal(text: &str) -> Option<Literal> {
    if let Some(imaginary) = text.strip_suffix('i') {
        let split = imaginary
            .char_indices()
            .filter(|(index, ch)| {
                *index > 0
                    && matches!(ch, '+' | '-')
                    && !matches!(imaginary.as_bytes()[index - 1], b'e' | b'E' | b'p' | b'P')
            })
            .map(|(index, _)| index)
            .next_back();
        let (real, imaginary) = if let Some(split) = split {
            (
                parse_float(&imaginary[..split])?,
                parse_float(&imaginary[split..])?,
            )
        } else {
            (0.0, parse_float(imaginary)?)
        };
        return (real.is_finite() && imaginary.is_finite())
            .then_some(Literal::Complex(real.to_bits(), imaginary.to_bits()));
    }
    if let Some((negative, magnitude)) = integer(text) {
        if negative {
            if magnitude <= 1u64 << 63 {
                return Some(Literal::Signed(
                    i64::from_ne_bytes(magnitude.to_ne_bytes()).wrapping_neg(),
                ));
            }
        } else if let Ok(value) = i64::try_from(magnitude) {
            return Some(Literal::Signed(value));
        } else {
            return Some(Literal::Unsigned(magnitude));
        }
        return None;
    }
    // Failed integer syntax is not reinterpreted as a decimal float unless
    // the source literal explicitly contains a radix point or exponent.
    if !text.contains(['.', 'e', 'E', 'p', 'P']) {
        return None;
    }
    let float = parse_float(text)?;
    float.is_finite().then_some(Literal::Float(float.to_bits()))
}

fn integer(text: &str) -> Option<(bool, u64)> {
    let negative = text.starts_with('-');
    let text = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (radix, digits, prefix) =
        if let Some(value) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
            (16, value, true)
        } else if let Some(value) = text.strip_prefix("0o").or_else(|| text.strip_prefix("0O")) {
            (8, value, true)
        } else if let Some(value) = text.strip_prefix("0b").or_else(|| text.strip_prefix("0B")) {
            (2, value, true)
        } else if text.len() > 1 && text.starts_with('0') {
            (8, text, false)
        } else {
            (10, text, false)
        };
    if !underscores(digits, radix, prefix) {
        return None;
    }
    u64::from_str_radix(&digits.replace('_', ""), radix)
        .ok()
        .map(|value| (negative, value))
}

/// Parses a binary64 value with Go's `strconv.ParseFloat` syntax and rounding.
///
/// Accepts decimal and hexadecimal floats, valid digit separators, and Go's
/// nonfinite spellings. Returns `None` for invalid syntax or finite overflow.
#[must_use]
pub fn parse_float(text: &str) -> Option<f64> {
    if text.eq_ignore_ascii_case("nan") {
        return Some(f64::NAN);
    }
    let negative = text.starts_with('-');
    let value = text.strip_prefix(['+', '-']).unwrap_or(text);
    if value.eq_ignore_ascii_case("inf") || value.eq_ignore_ascii_case("infinity") {
        return Some(if negative {
            f64::NEG_INFINITY
        } else {
            f64::INFINITY
        });
    }
    if let Some(hex) = value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
    {
        return hexadecimal(hex, negative).filter(|value| value.is_finite());
    }
    let (mantissa, exponent) = value
        .split_once(['e', 'E'])
        .map_or((value, None), |(mantissa, exponent)| {
            (mantissa, Some(exponent))
        });
    if !underscores(mantissa, 10, false) || mantissa.matches('.').count() > 1 {
        return None;
    }
    if let Some(exponent) = exponent {
        let exponent = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        if exponent.is_empty()
            || !underscores(exponent, 10, false)
            || !exponent.chars().all(|ch| ch.is_ascii_digit() || ch == '_')
        {
            return None;
        }
    }
    if !mantissa
        .chars()
        .all(|ch| ch.is_ascii_digit() || matches!(ch, '_' | '.'))
        || !mantissa.chars().any(|ch| ch.is_ascii_digit())
    {
        return None;
    }
    text.replace('_', "")
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn underscores(value: &str, radix: u32, prefix: bool) -> bool {
    let digit = |byte: u8| char::from(byte).is_digit(radix);
    value.as_bytes().iter().enumerate().all(|(index, byte)| {
        *byte != b'_'
            || (value
                .as_bytes()
                .get(index + 1)
                .is_some_and(|byte| digit(*byte))
                && (if index == 0 {
                    prefix
                } else {
                    digit(value.as_bytes()[index - 1])
                }))
    })
}

fn hexadecimal(hex: &str, negative: bool) -> Option<f64> {
    let (mantissa, exponent) = hex.split_once(['p', 'P'])?;
    if !underscores(mantissa, 16, true) || mantissa.matches('.').count() > 1 {
        return None;
    }
    let exponent_negative = exponent.starts_with('-');
    let exponent = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
    if exponent.is_empty()
        || !underscores(exponent, 10, false)
        || !exponent.chars().all(|ch| ch.is_ascii_digit() || ch == '_')
    {
        return None;
    }
    let exponent = exponent
        .chars()
        .filter(|ch| *ch != '_')
        .fold(0i64, |value, ch| {
            value
                .saturating_mul(10)
                .saturating_add(i64::from(ch.to_digit(10).expect("decimal exponent")))
                .min(100_000)
        });
    let exponent = if exponent_negative {
        -exponent
    } else {
        exponent
    };
    let mut digits = 0i64;
    let mut point = None;
    let mut leading = 0i64;
    let mut kept = 0i64;
    let mut bits = 0u64;
    let mut sticky = false;
    for ch in mantissa.chars().filter(|ch| *ch != '_') {
        if ch == '.' {
            point = Some(digits);
            continue;
        }
        let digit = ch.to_digit(16)?;
        digits += 1;
        if bits == 0 && digit == 0 && kept == 0 {
            leading += 1;
            continue;
        }
        if kept < 16 {
            bits = (bits << 4) | u64::from(digit);
            kept += 1;
        } else {
            sticky |= digit != 0;
        }
    }
    if digits == 0 {
        return None;
    }
    let sign = u64::from(negative) << 63;
    if bits == 0 {
        return Some(f64::from_bits(sign));
    }
    let power = exponent + 4 * (point.unwrap_or(digits) - leading - kept);
    let width = 64 - i64::from(bits.leading_zeros());
    let mut binary_exponent = power + width - 1;
    let rounded = if binary_exponent >= -1022 {
        let mut rounded = round(bits, width - 53, sticky);
        if rounded == 1u64 << 53 {
            rounded >>= 1;
            binary_exponent += 1;
        }
        if binary_exponent > 1023 {
            return Some(f64::from_bits(sign | 0x7ff0_0000_0000_0000));
        }
        (u64::try_from(binary_exponent + 1023).expect("normal binary exponent") << 52)
            | (rounded & ((1u64 << 52) - 1))
    } else {
        round(bits, -1074 - power, sticky)
    };
    Some(f64::from_bits(sign | rounded))
}
fn round(bits: u64, shift: i64, sticky: bool) -> u64 {
    if shift <= 0 {
        return bits << u32::try_from(-shift).expect("bounded significand shift");
    }
    if shift > 64 {
        return 0;
    }
    let quotient = if shift == 64 { 0 } else { bits >> shift };
    let remainder = if shift == 64 {
        bits
    } else {
        bits & ((1u64 << shift) - 1)
    };
    let half = 1u64 << (shift - 1);
    quotient + u64::from(remainder > half || (remainder == half && (sticky || quotient & 1 == 1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hexadecimal_float_rounding_matches_independent_go_bit_patterns() {
        for (text, bits) in [
            ("0x1.00000000000008p0", 0x3ff0_0000_0000_0000),
            ("0x1.00000000000008001p0", 0x3ff0_0000_0000_0001),
            ("0x1.00000000000018p0", 0x3ff0_0000_0000_0002),
            ("0x0.0000000000001p-1022", 1),
            ("0x0.00000000000008p-1022", 0),
            ("0x0.00000000000008001p-1022", 1),
            ("0x1.fffffffffffffp1023", 0x7fef_ffff_ffff_ffff),
            ("-0x0p-100000", 0x8000_0000_0000_0000),
            ("1_2.3_4e5_6", 0x4bc9_29c7_d37d_0d30),
        ] {
            assert2::check!(parse_float(text).unwrap().to_bits() == bits);
        }
        for text in [
            "0x1.fffffffffffff8p1023",
            "0x1p+100000",
            "1e400",
            "+NaN",
            "-NaN",
            "1__2",
            "1_.2",
            "1._2",
            "1e_2",
            "0x1",
            "0x_1_p2",
            "0x1p_2",
            "0x.p2",
            "1.2.3",
        ] {
            assert2::check!(parse_float(text).is_none());
        }
    }
    #[test]
    fn ideal_constants_preserve_types_and_parse_versus_execution_overflow_boundaries() {
        assert2::check!(literal(".5") == Some(Literal::Float(0.5f64.to_bits())));
        assert2::check!(literal("1.") == Some(Literal::Float(1.0f64.to_bits())));
        assert2::check!(literal("0755") == Some(Literal::Signed(493)));
        assert2::check!(literal("0x_1.fp2") == Some(Literal::Float(7.75f64.to_bits())));
        assert2::check!(
            literal("2i") == Some(Literal::Complex(0.0f64.to_bits(), 2.0f64.to_bits()))
        );
        assert2::check!(
            literal("1+2i") == Some(Literal::Complex(1.0f64.to_bits(), 2.0f64.to_bits()))
        );
        assert2::check!(literal("9223372036854775808") == Some(Literal::Unsigned(1u64 << 63)));
        assert2::check!(literal("18446744073709551615") == Some(Literal::Unsigned(u64::MAX)));
        for text in [
            "18446744073709551616",
            "08",
            "1e309",
            "-9223372036854775809",
            "1+2",
            "1+-2i",
        ] {
            assert2::check!(literal(text).is_none());
        }
    }
}
