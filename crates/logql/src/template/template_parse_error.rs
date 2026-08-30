use super::ParseError;

pub(crate) fn template_parse_error(message: &str) -> ParseError {
    ParseError::Syntax {
        message: message.to_string(),
        position: 0,
    }
}
