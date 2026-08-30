use super::*;

pub(crate) fn decode_quoted_escape(escaped: char) -> char {
    match escaped {
        'n' => '\n',
        'r' => '\r',
        't' => '\t',
        '"' => '"',
        '\\' => '\\',
        other => other,
    }
}
