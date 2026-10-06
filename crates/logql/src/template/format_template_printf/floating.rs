use super::Spec;

pub(super) fn render(value: f64, verb: char, mut spec: Spec) -> Option<String> {
    if !matches!(
        verb,
        'v' | 'b' | 'e' | 'E' | 'f' | 'F' | 'g' | 'G' | 'x' | 'X'
    ) {
        return None;
    }
    let negative = value.is_sign_negative() && !value.is_nan();
    let sign = if negative {
        "-"
    } else if spec.flags[Spec::PLUS] {
        "+"
    } else if spec.flags[Spec::SPACE] {
        " "
    } else {
        ""
    };
    if !value.is_finite() {
        spec.flags[Spec::ZERO] = false;
        return Some(spec.pad(&if value.is_nan() {
            format!("{sign}NaN")
        } else {
            format!("{}Inf", if sign.is_empty() { "+" } else { sign })
        }));
    }
    let absolute = value.abs();
    let precision = spec.precision.unwrap_or(6);
    let mut body = match verb {
        'b' => binary(absolute),
        'f' | 'F' => format!("{absolute:.precision$}"),
        'e' | 'E' => exponent(
            &format!("{absolute:.precision$e}"),
            if verb == 'E' { 'E' } else { 'e' },
        ),
        'x' | 'X' => hex(absolute, spec.precision, verb == 'X'),
        _ => general(
            absolute,
            spec.precision,
            matches!(verb, 'G'),
            spec.flags[Spec::SHARP],
        ),
    };
    if spec.flags[Spec::SHARP] && verb != 'b' {
        let exponent_at = body
            .find(if matches!(verb, 'x' | 'X') {
                &['p', 'P'][..]
            } else {
                &['e', 'E'][..]
            })
            .unwrap_or(body.len());
        if !body[..exponent_at].contains('.') {
            body.insert(exponent_at, '.');
        }
        if verb == 'x' && spec.precision.is_none() {
            // fmt's default sharp-x precision counts its 0x prefix and leading
            // significand digit too, as the pinned source's digit loop does.
            let end = body.find(['p', 'P']).unwrap_or(body.len());
            let digits = body[..end]
                .chars()
                .filter(|ch| *ch != '.')
                .count()
                .saturating_sub(1);
            if digits < 6 {
                body.insert_str(end, &"0".repeat(6 - digits));
            }
        }
    }
    let rendered = format!("{sign}{body}");
    if spec.flags[Spec::ZERO] && !spec.flags[Spec::MINUS] && !sign.is_empty() {
        let padding = spec.width.unwrap_or(0).saturating_sub(rendered.len());
        Some(format!("{sign}{}{body}", "0".repeat(padding)))
    } else {
        Some(spec.pad(&rendered))
    }
}

fn exponent(value: &str, marker: char) -> String {
    let Some((mantissa, exponent)) = value.split_once('e') else {
        return value.into();
    };
    let exponent: i32 = exponent.parse().unwrap_or(0);
    format!(
        "{mantissa}{marker}{}{exponent:02}",
        if exponent < 0 { "-" } else { "+" },
        exponent = exponent.unsigned_abs()
    )
}

fn general(value: f64, precision: Option<usize>, upper: bool, sharp: bool) -> String {
    let digits = precision.map(|precision| precision.max(1));
    let scientific = digits.map_or_else(
        || format!("{value:e}"),
        |digits| format!("{value:.precision$e}", precision = digits - 1),
    );
    let (mantissa, power) = scientific.split_once('e').unwrap_or((&scientific, "0"));
    let power: i32 = power.parse().unwrap_or(0);
    let threshold = digits.unwrap_or(6);
    let marker = if upper { 'E' } else { 'e' };
    let mut result = if power < -4
        || power >= i32::try_from(threshold).expect("formatter precision fits int32")
    {
        let mantissa = if sharp {
            mantissa.to_owned()
        } else {
            trim(mantissa)
        };
        exponent(&format!("{mantissa}e{power}"), marker)
    } else if let Some(digits) = digits {
        let places = usize::try_from(
            (i64::try_from(digits).expect("formatter precision fits int64") - i64::from(power) - 1)
                .max(0),
        )
        .expect("nonnegative decimal places fit usize");
        let fixed = format!("{value:.places$}");
        if sharp { fixed } else { trim(&fixed) }
    } else {
        value.to_string()
    };
    if sharp {
        let end = result.find(['e', 'E']).unwrap_or(result.len());
        let mut significant = result[..end]
            .chars()
            .filter(char::is_ascii_digit)
            .skip_while(|ch| *ch == '0')
            .count();
        if significant == 0 {
            significant = 1;
        }
        let needed = digits.unwrap_or(6).saturating_sub(significant);
        if !result[..end].contains('.') {
            result.insert(end, '.');
        }
        let end = result.find(['e', 'E']).unwrap_or(result.len());
        result.insert_str(end, &"0".repeat(needed));
    }
    result
}

fn trim(value: &str) -> String {
    if value.contains('.') {
        value.trim_end_matches('0').trim_end_matches('.').into()
    } else {
        value.into()
    }
}

fn binary(value: f64) -> String {
    let bits = value.to_bits();
    let biased = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    let mantissa = if biased == 0 {
        fraction
    } else {
        fraction | (1_u64 << 52)
    };
    format!(
        "{mantissa}p{}{power}",
        if biased == 0 || biased < 1075 {
            "-"
        } else {
            "+"
        },
        power = if biased == 0 {
            1074
        } else {
            (biased - 1075).unsigned_abs()
        }
    )
}

fn hex(value: f64, precision: Option<usize>, upper: bool) -> String {
    let bits = value.to_bits();
    let biased = ((bits >> 52) & 0x7ff) as i32;
    let fraction = bits & ((1_u64 << 52) - 1);
    let mut mantissa = if biased == 0 {
        fraction
    } else {
        fraction | (1_u64 << 52)
    };
    let mut power = if biased == 0 { -1022 } else { biased - 1023 };
    if mantissa != 0 {
        while mantissa < (1_u64 << 52) {
            mantissa <<= 1;
            power -= 1;
        }
    } else {
        power = 0;
    }
    if let Some(digits) = precision.filter(|digits| *digits < 13) {
        let shift = 52 - 4 * digits;
        let remainder = mantissa & ((1_u64 << shift) - 1);
        let halfway = 1_u64 << (shift - 1);
        let mut rounded = mantissa >> shift;
        if remainder > halfway || (remainder == halfway && rounded & 1 != 0) {
            rounded += 1;
        }
        mantissa = rounded << shift;
        if mantissa >= (1_u64 << 53) {
            mantissa >>= 1;
            power += 1;
        }
    }
    let digit = mantissa >> 52;
    let fraction = mantissa & ((1_u64 << 52) - 1);
    let mut tail = format!("{fraction:013x}");
    if let Some(precision) = precision {
        if precision <= 13 {
            tail.truncate(precision);
        } else {
            tail.push_str(&"0".repeat(precision - 13));
        }
    } else {
        tail = tail.trim_end_matches('0').into();
    }
    let point = if tail.is_empty() {
        String::new()
    } else {
        format!(".{tail}")
    };
    let result = format!(
        "0x{digit}{point}p{}{power:02}",
        if power < 0 { "-" } else { "+" },
        power = power.unsigned_abs()
    );
    if upper {
        result.to_ascii_uppercase()
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn go_extreme_values_rounding_and_negative_zero() {
        // Independently captured from Go1.26.5 fmt source (byte-identical to
        // host 1.26.0), including subnormal and hexadecimal carry boundaries.
        for (value, expected) in [
            (
                f64::from_bits(1),
                "5e-324|4.94e-324|5.00000e-324|0x1p-1074|0x1p-1074|1p-1074",
            ),
            (
                f64::MAX,
                "1.7976931348623157e+308|1.8e+308|1.7976931348623157e+308|0x1.fffffffffffffp+1023|0x1p+1024|9007199254740991p+971",
            ),
            (-0.0, "-0|-0|-0.00000|-0x0p+00|-0x0p+00|-0p-1074"),
            (f64::INFINITY, "+Inf|+Inf|+Inf|+Inf|+Inf|+Inf"),
            (f64::NAN, "NaN|NaN|NaN|NaN|NaN|NaN"),
            (
                9.999,
                "9.999|10|9.99900|0x1.3ff7ced916873p+03|0x1p+03|5628936584259699p-49",
            ),
            (
                999_999.5,
                "999999.5|1e+06|999999.5|0x1.e847fp+19|0x1p+20|8589930297032704p-33",
            ),
        ] {
            let formats = [
                ('g', Spec::default()),
                (
                    'g',
                    Spec {
                        precision: Some(3),
                        ..Spec::default()
                    },
                ),
                ('g', Spec { ..Spec::default() }.with_flag(Spec::SHARP, true)),
                ('x', Spec::default()),
                (
                    'x',
                    Spec {
                        precision: Some(0),
                        ..Spec::default()
                    },
                ),
                ('b', Spec::default()),
            ];
            let actual = formats
                .into_iter()
                .map(|(verb, spec)| render(value, verb, spec).unwrap())
                .collect::<Vec<_>>()
                .join("|");
            check!(actual == expected);
        }
    }
}
