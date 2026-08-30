
pub(crate) fn traceql_tag_field(key: &str) -> String {
    if key.contains(':') {
        key.to_string()
    } else {
        format!(".{}", key.strip_prefix('.').unwrap_or(key))
    }
}
