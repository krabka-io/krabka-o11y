use super::{ParseError, template_parse_error};

struct EscapeNumber {
    radix: u32,
    digits: u8,
    value: u32,
}

pub(crate) fn decode_quoted_fragment(fragment: &str) -> Result<String, ParseError> {
    String::from_utf8(decode_quoted_bytes(fragment, '"')?)
        .map_err(|_| template_parse_error("template name must be valid UTF-8"))
}

pub(crate) fn decode_quoted_bytes(fragment: &str, quote: char) -> Result<Vec<u8>, ParseError> {
    let mut decoded = Vec::new();
    let mut chars = fragment.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            if ch == '\n' || ch == '\r' {
                return Err(template_parse_error("newline in quoted constant"));
            }
            let mut buffer = [0; 4];
            decoded.extend(ch.encode_utf8(&mut buffer).as_bytes());
            continue;
        }
        let escaped = chars
            .next()
            .ok_or_else(|| template_parse_error("unterminated template escape"))?;
        match escaped {
            '\\' => decoded.push(b'\\'),
            '"' if quote == '"' => decoded.push(b'"'),
            '\'' if quote == '\'' => decoded.push(b'\''),
            'a' => decoded.push(7),
            'b' => decoded.push(8),
            'f' => decoded.push(12),
            'n' => decoded.push(b'\n'),
            'r' => decoded.push(b'\r'),
            't' => decoded.push(b'\t'),
            'v' => decoded.push(11),
            'x' | 'u' | 'U' | '0'..='7' => {
                let mut number = match escaped {
                    'x' => EscapeNumber {
                        radix: 16,
                        digits: 2,
                        value: 0,
                    },
                    'u' => EscapeNumber {
                        radix: 16,
                        digits: 4,
                        value: 0,
                    },
                    'U' => EscapeNumber {
                        radix: 16,
                        digits: 8,
                        value: 0,
                    },
                    digit => EscapeNumber {
                        radix: 8,
                        digits: 2,
                        value: digit.to_digit(8).expect("octal escape"),
                    },
                };
                for _ in 0..number.digits {
                    let digit = chars
                        .next()
                        .and_then(|ch| ch.to_digit(number.radix))
                        .ok_or_else(|| template_parse_error("invalid template escape"))?;
                    number.value = number
                        .value
                        .checked_mul(number.radix)
                        .and_then(|value| value.checked_add(digit))
                        .ok_or_else(|| template_parse_error("invalid Unicode escape"))?;
                }
                if escaped == 'x' || number.radix == 8 {
                    decoded.push(
                        u8::try_from(number.value)
                            .map_err(|_| template_parse_error("byte escape exceeds 255"))?,
                    );
                } else {
                    let ch = char::from_u32(number.value)
                        .ok_or_else(|| template_parse_error("invalid Unicode scalar"))?;
                    let mut buffer = [0; 4];
                    decoded.extend(ch.encode_utf8(&mut buffer).as_bytes());
                }
            }
            _ => return Err(template_parse_error("unknown template escape")),
        }
    }
    Ok(decoded)
}
