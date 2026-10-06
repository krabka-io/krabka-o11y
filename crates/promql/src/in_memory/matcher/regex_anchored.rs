use super::{PromqlError, Result};

pub(crate) fn regex_anchored(pattern: &str) -> Result<regex::Regex> {
    regex::Regex::new(&format!("^(?s:{pattern})$"))
        .map_err(|error| PromqlError::Plan(format!("bad regex `{pattern}`: {error}")))
}
