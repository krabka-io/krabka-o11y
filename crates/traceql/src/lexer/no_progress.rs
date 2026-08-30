use super::*;

pub(crate) fn no_progress(pos: usize) -> TraceqlError {
    TraceqlError::Parse(format!("lexer made no progress at byte {pos}"))
}
