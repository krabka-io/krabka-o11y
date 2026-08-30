use super::{ParseError, template_parse_error};

pub(crate) fn decode_quoted_fragment(fragment: &str) -> Result<String, ParseError> {
    let mut decoded = String::new();
    let mut chars = fragment.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(template_parse_error("unterminated template escape"));
        };
        match escaped {
            '\\' => decoded.push('\\'),
            '"' => decoded.push('"'),
            'n' => decoded.push('\n'),
            'r' => decoded.push('\r'),
            't' => decoded.push('\t'),
            other => decoded.push(other),
        }
    }
    Ok(decoded)
}
