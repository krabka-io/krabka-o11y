use super::decode_quoted_escape;

/// Why [`read_quoted_body`] stopped before the closing quote.
pub(crate) enum QuotedBodyError {
    /// The input ended right after a backslash.
    DanglingEscape,
    /// The input ended before the closing quote.
    Unterminated,
}

/// Decodes a double-quoted body whose first character is at `*pos`, just
/// after the opening quote, and advances `*pos` past the closing quote.
pub(crate) fn read_quoted_body(input: &str, pos: &mut usize) -> Result<String, QuotedBodyError> {
    let peek = |pos: usize| input[pos..].chars().next();
    let mut out = String::new();
    while let Some(ch) = peek(*pos) {
        *pos = pos.saturating_add(ch.len_utf8());
        match ch {
            '"' => return Ok(out),
            '\\' => {
                let Some(escaped) = peek(*pos) else {
                    return Err(QuotedBodyError::DanglingEscape);
                };
                *pos = pos.saturating_add(escaped.len_utf8());
                out.push(decode_quoted_escape(escaped));
            }
            _ => out.push(ch),
        }
    }
    Err(QuotedBodyError::Unterminated)
}
