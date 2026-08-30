use super::{Result, TraceqlError, no_progress};

pub(crate) fn advance(input: &str, pos: usize, len: usize) -> Result<usize> {
    if len == 0 {
        return Err(no_progress(pos));
    }
    let next = pos
        .checked_add(len)
        .ok_or_else(|| TraceqlError::Parse(format!("lexer position overflow at byte {pos}")))?;
    if next > input.len() {
        return Err(TraceqlError::Parse(format!(
            "lexer advanced past end of input at byte {pos}"
        )));
    }
    Ok(next)
}
