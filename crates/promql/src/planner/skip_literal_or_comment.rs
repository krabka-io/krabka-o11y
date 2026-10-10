/// Steps a byte scanner of a `PromQL` query over string literals and comments.
///
/// Returns `true` when the byte at `index` belongs to a string literal or a
/// `#` comment: the scanner has advanced `index` past it (to the newline that
/// ends a comment) and tracked the open literal's delimiter in `quote`.
/// Returns `false`, leaving `index` alone, for a byte of query code.
pub(crate) fn skip_literal_or_comment(
    quote: &mut Option<u8>,
    bytes: &[u8],
    index: &mut usize,
) -> bool {
    let byte = bytes[*index];
    if let Some(delimiter) = *quote {
        if byte == b'\\' && delimiter != b'`' {
            *index += 2;
            return true;
        }
        if byte == delimiter {
            *quote = None;
        }
    } else if matches!(byte, b'"' | b'\'' | b'`') {
        *quote = Some(byte);
    } else if byte == b'#' {
        while *index < bytes.len() && bytes[*index] != b'\n' {
            *index += 1;
        }
        return true;
    } else {
        return false;
    }
    *index += 1;
    true
}
