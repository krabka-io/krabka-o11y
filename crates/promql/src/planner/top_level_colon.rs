use super::{PromqlError, Result};

/// The offset of a range expression's subquery colon, in bytes from the start
/// of `content`, or `None` when the content holds no colon outside a string or
/// a parenthesis.
///
/// The offset counts bytes rather than characters so that the caller can slice
/// `content` with it. A character count disagrees with a byte count as soon as
/// anything before the colon is multi-byte, and slicing with the wrong one
/// lands inside a character and panics -- on a query string that arrives
/// unauthenticated.
pub(crate) fn top_level_colon(content: &str) -> Result<Option<usize>> {
    let mut parens = 0_i32;
    let mut quote = None;
    for (offset, ch) in content.char_indices() {
        if let Some(quote_ch) = quote {
            if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '\'' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => parens -= 1,
            ':' if parens == 0 => return Ok(Some(offset)),
            _ => {}
        }
        if parens < 0 {
            return Err(PromqlError::Parse(format!(
                "unbalanced duration expression `{content}`"
            )));
        }
    }
    Ok(None)
}
