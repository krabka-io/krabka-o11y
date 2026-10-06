pub(crate) fn parse_template_integer(value: &str) -> i64 {
    let value = if let Some((whole, fraction)) = value.split_once('.') {
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
    let (negative, digits) = value.strip_prefix('-').map_or(
        (false, value.strip_prefix('+').unwrap_or(value)),
        |digits| (true, digits),
    );
    let (radix, digits) = if let Some(digits) = digits
        .strip_prefix("0x")
        .or_else(|| digits.strip_prefix("0X"))
    {
        (16, digits)
    } else if let Some(digits) = digits
        .strip_prefix("0b")
        .or_else(|| digits.strip_prefix("0B"))
    {
        (2, digits)
    } else if let Some(digits) = digits
        .strip_prefix("0o")
        .or_else(|| digits.strip_prefix("0O"))
    {
        (8, digits)
    } else if digits.len() > 1 && digits.starts_with('0') {
        (8, digits)
    } else {
        (10, digits)
    };
    let signed = if negative {
        format!("-{digits}")
    } else {
        digits.to_string()
    };
    let bytes = digits.as_bytes();
    if bytes.iter().enumerate().any(|(index, byte)| {
        *byte == b'_'
            && (index + 1 == bytes.len() || bytes[index + 1] == b'_' || (index == 0 && radix == 10))
    }) {
        return None;
    }
    i64::from_str_radix(&signed.replace('_', ""), radix).ok()
}
