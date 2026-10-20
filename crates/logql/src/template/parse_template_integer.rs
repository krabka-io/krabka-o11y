pub(crate) fn parse_template_integer(value: &str) -> i64 {
    let value = if let Some(parts) = value.split_once('.') {
        let whole = parts.0;
        let fraction = parts.1;
        if whole
            .trim_start_matches(['+', '-'])
            .chars()
            .all(|ch| ch.is_ascii_digit())
            && fraction.chars().all(|ch| ch.is_ascii_digit())
        {
            match whole {
                "" | "+" | "-" => "0",
                _ => whole,
            }
        } else {
            value
        }
    } else {
        value
    };
    parse_template_integer_literal(value).unwrap_or_default()
}

pub(crate) fn parse_template_integer_literal(value: &str) -> Option<i64> {
    struct SignedDigits<'a> {
        negative: bool,
        digits: &'a str,
    }
    struct RadixDigits<'a> {
        radix: u32,
        digits: &'a str,
    }

    let signed_digits = value.strip_prefix('-').map_or(
        SignedDigits {
            negative: false,
            digits: value.strip_prefix('+').unwrap_or(value),
        },
        |digits| SignedDigits {
            negative: true,
            digits,
        },
    );
    let negative = signed_digits.negative;
    let digits = signed_digits.digits;
    let radix_digits = if let Some(digits) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        RadixDigits { radix: 16, digits }
    } else if let Some(digits) = digits
        .strip_prefix("0b")
        .or_else(|| digits.strip_prefix("0B"))
    {
        RadixDigits { radix: 2, digits }
    } else if let Some(digits) = digits
        .strip_prefix("0o")
        .or_else(|| digits.strip_prefix("0O"))
    {
        RadixDigits { radix: 8, digits }
    } else if digits.len() > 1 && digits.starts_with('0') {
        RadixDigits { radix: 8, digits }
    } else {
        RadixDigits { radix: 10, digits }
    };
    let radix = radix_digits.radix;
    let digits = radix_digits.digits;
    let signed = if negative {
        format!("-{digits}")
    } else {
        digits.to_string()
    };
    let bytes = digits.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'_'
            && (index + 1 == bytes.len() || bytes[index + 1] == b'_' || (index == 0 && radix == 10))
        {
            return None;
        }
        index += 1;
    }
    i64::from_str_radix(&signed.replace('_', ""), radix).ok()
}
