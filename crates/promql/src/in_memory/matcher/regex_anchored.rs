use super::{Result, PromqlError};

pub(crate) fn regex_anchored(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(&format!("^(?:{pattern})$"))
        .map_err(|error| PromqlError::Plan(format!("bad regex `{pattern}`: {error}")))
}
