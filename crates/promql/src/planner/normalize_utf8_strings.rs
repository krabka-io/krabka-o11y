use std::collections::BTreeMap;

use super::{PromqlError, Result};
use crate::PromqlString;

// Go string byte escapes form UTF-8 sequences; the dependency decodes each
// escaped byte as a separate Unicode character. Normalize before parsing.
pub(crate) fn normalize_utf8_strings(
    query: &str,
) -> Result<(String, BTreeMap<String, PromqlString>)> {
    let bytes = query.as_bytes();
    let mut literals = Vec::new();
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
        let decoded = PromqlString::from(decode(raw).map_err(PromqlError::Parse)?);
        if decoded.utf8().is_none()
            && (query[..start].trim_end().ends_with("=~")
                || query[..start].trim_end().ends_with("!~"))
        {
            return Err(PromqlError::Parse(
                "error parsing regexp: invalid UTF-8".to_owned(),
            ));
        }
        if decoded.utf8().is_none() && query[index..].trim_start().starts_with(['=', '!']) {
            return Err(PromqlError::Parse("invalid UTF-8 label name".to_owned()));
        }
        literals.push((start, index, decoded));
    }
    // Markers exist only while the dependency parses its UTF-8 StringLiteral.
    // They are restored to typed byte nodes before evaluation or formatting.
    // Exclude both source spellings and all decoded values to avoid aliasing a
    // genuine query literal that spells the same bytes via escapes.
    let mut prefix = "__krabka_byte_literal_".to_owned();
    while query.contains(&prefix)
        || literals.iter().any(|(_, _, value)| {
            value
                .as_bytes()
                .windows(prefix.len())
                .any(|window| window == prefix.as_bytes())
        })
    {
        prefix.push('_');
    }
    let mut output = String::new();
    let mut values = BTreeMap::new();
    let mut copied = 0;
    for (start, end, value) in literals {
        output.push_str(&query[copied..start]);
        if let Some(utf8) = value.utf8() {
            output.push_str(&serde_json::to_string(utf8).expect("string serialization succeeds"));
        } else {
            let marker = format!("{prefix}{}", values.len());
            output
                .push_str(&serde_json::to_string(&marker).expect("string serialization succeeds"));
            values.insert(marker, value);
        }
        copied = end;
    }
    output.push_str(&query[copied..]);
    Ok((output, values))
}

fn decode(raw: &str) -> std::result::Result<Vec<u8>, String> {
    if raw.starts_with('`') {
        return Ok(raw[1..raw.len() - 1].replace('\r', "").into_bytes());
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
    Ok(decoded)
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
        let promql_parser::parser::Expr::Call(call) =
            promql_parser::parser::parse(&query.0).unwrap()
        else {
            panic!("call expected")
        };
        let promql_parser::parser::Expr::StringLiteral(label) = call.args.args[1].as_ref() else {
            panic!("string expected")
        };
        assert2::assert!(label.val == "é");
        assert2::assert!(
            normalize_utf8_strings(r#"count_values("\xff", up)"#)
                .unwrap()
                .1
                .values()
                .next()
                .unwrap()
                .as_bytes()
                == &[0xff]
        );
        assert2::assert!(
            normalize_utf8_strings(r#"count_values("\\xff", up)"#)
                .unwrap()
                .0
                == r#"count_values("\\xff", up)"#
        );
    }
}
