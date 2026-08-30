use super::*;

pub(crate) fn parse_error(line: Line<'_>, message: impl Into<String>) -> PromqlError {
    PromqlError::Parse(format!("line {}: {}", line.number, message.into()))
}
