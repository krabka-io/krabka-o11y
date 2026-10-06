use super::{PromqlError, Result};

// Go string byte escapes form UTF-8 sequences; the dependency decodes each
// escaped byte as a separate Unicode character. Normalize before parsing.
pub(crate) fn normalize_utf8_strings(query: &str) -> Result<String> {
    let bytes = query.as_bytes();
    let mut output = String::new();
    let mut copied = 0;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'#' {
            while index < bytes.len() && bytes[index] != b'\n' {
                index += 1;
            }
            continue;
        }
        if !matches!(bytes[index], b'"' | b'\'' | b'`') {
            index += 1;
            continue;
        }
        let start = index;
        let delimiter = bytes[index];
        index += 1;
        while index < bytes.len() && bytes[index] != delimiter {
            if bytes[index] == b'\\' && delimiter != b'`' {
                index += 1;
            }
            index += 1;
        }
        if index >= bytes.len() {
            return Err(PromqlError::Parse("unterminated string".to_string()));
        }
        index += 1;
        let raw = &query[start..index];
        let decoded = decode(raw).map_err(|error| {
            let prefix = query[..start].trim_end();
            let label_argument = prefix
                .strip_suffix('(')
                .is_some_and(|prefix| prefix.trim_end().ends_with("count_values"));
            let invalid_utf8 = error.starts_with("invalid UTF-8 string:");
            let message = if label_argument {
                format!("invalid label name {raw}")
            } else {
                error
            };
            // Go accepts byte escapes in strings. Invalid UTF-8 label names
            // fail during evaluation, rather than as malformed query syntax.
            if invalid_utf8 {
                PromqlError::Exec(message)
            } else {
                PromqlError::Parse(message)
            }
        })?;
        output.push_str(&query[copied..start]);
        output.push_str(&serde_json::to_string(&decoded).expect("string serialization succeeds"));
        copied = index;
    }
    output.push_str(&query[copied..]);
    Ok(output)
}

fn decode(raw: &str) -> std::result::Result<String, String> {
    if raw.starts_with('`') {
        return Ok(raw[1..raw.len() - 1].replace('\r', ""));
    }
    let bytes = raw.as_bytes();
    let mut decoded = Vec::new();
    let mut index = 1;
    while index < bytes.len() - 1 {
        if bytes[index] != b'\\' {
            decoded.push(bytes[index]);
            index += 1;
            continue;
        }
        let next = *bytes.get(index + 1).ok_or("incomplete escape")?;
        let length = match next {
            b'x' | b'0'..=b'7' => 4,
            b'u' => 6,
            b'U' => 10,
            _ => 2,
        };
        let end = index + length;
        if end > bytes.len() - 1 {
            return Err("incomplete escape".to_string());
        }
        if next == b'x' || next.is_ascii_digit() {
            let offset = if next == b'x' { 2 } else { 1 };
            let radix = if next == b'x' { 16 } else { 8 };
            decoded.push(
                u8::from_str_radix(
                    raw.get(index + offset..end).ok_or("invalid byte escape")?,
                    radix,
                )
                .map_err(|error| error.to_string())?,
            );
        } else {
            decoded.extend_from_slice(
                promql_parser::util::unquote_string(&format!(
                    "\"{}\"",
                    raw.get(index..end).ok_or("invalid string escape")?
                ))?
                .as_bytes(),
            );
        }
        index = end;
    }
    String::from_utf8(decoded).map_err(|error| format!("invalid UTF-8 string: {error}"))
}

#[cfg(test)]
mod tests {
    use super::normalize_utf8_strings;
    #[test]
    fn malformed_escapes_reject_without_slicing_beyond_utf8_boundaries() {
        for query in ["'\\", "\"\\", r#""\é""#, r#""\u🦀""#] {
            assert2::assert!(normalize_utf8_strings(query).is_err(), "{query:?}");
        }
    }

    #[test]
    fn byte_escapes_form_unicode_and_do_not_turn_literal_backslashes_into_escapes() {
        let query =
            normalize_utf8_strings(r#"label_replace(up, "\xc3\xa9", "", "src", "(.*)")"#).unwrap();
        let promql_parser::parser::Expr::Call(call) = promql_parser::parser::parse(&query).unwrap()
        else {
            panic!("call expected")
        };
        let promql_parser::parser::Expr::StringLiteral(label) = call.args.args[1].as_ref() else {
            panic!("string expected")
        };
        assert2::assert!(label.val == "é");
        assert2::assert!(normalize_utf8_strings(r#"count_values("\xff", up)"#).is_err());
        assert2::assert!(
            normalize_utf8_strings(r#"count_values("\\xff", up)"#).unwrap()
                == r#"count_values("\\xff", up)"#
        );
    }
}
