pub(crate) fn anchored(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}
