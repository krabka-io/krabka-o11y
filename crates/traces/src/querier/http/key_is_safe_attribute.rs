/// A legacy `tags=` key is a safe `TraceQL` attribute reference only if it is
/// made of identifier characters: alphanumerics plus `._:-`.
///
/// Any other character, such as `{`, `}`, `"`, `\`, `|`, `&`, `=` or
/// whitespace, could inject query structure once it is interpolated unquoted
/// into the generated `TraceQL`.
pub(crate) fn key_is_safe_attribute(key: &str) -> bool {
    !key.is_empty()
        && key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-'))
}
