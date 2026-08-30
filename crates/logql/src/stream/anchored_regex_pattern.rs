
pub(crate) fn anchored_regex_pattern(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}
