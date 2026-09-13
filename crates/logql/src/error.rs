use thiserror::Error;

#[derive(Debug, Error)]
/// A LogQL syntax or filter-construction error.
pub enum ParseError {
    #[error("invalid regex `{pattern}`: {source}")]
    InvalidRegex {
        pattern: String,
        source: regex::Error,
    },
    #[error("{message} at byte {position}")]
    Syntax { message: String, position: usize },
}
