use super::pattern_value_is_variable;

pub(crate) fn log_pattern_token(token: &str) -> String {
    let Some(separator) = token.find('=') else {
        return if pattern_value_is_variable(token) {
            "<_>".to_string()
        } else {
            token.to_string()
        };
    };
    let key = &token[..separator];
    let value = &token[separator + 1..];
    if key.is_empty() || value.is_empty() {
        return token.to_string();
    }
    if pattern_value_is_variable(value) {
        format!("{key}=<_>")
    } else {
        token.to_string()
    }
}
