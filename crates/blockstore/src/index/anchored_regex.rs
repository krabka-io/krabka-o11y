use super::*;

pub(crate) fn anchored_regex(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}
