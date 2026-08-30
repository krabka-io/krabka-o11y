use super::ParseError;

pub(crate) fn syntax_error(message: &str) -> ParseError {
    ParseError::Syntax {
        message: message.to_string(),
        position: 0,
    }
}
