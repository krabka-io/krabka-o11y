use super::*;

pub(crate) fn log_pattern_token(token: &str) -> String {
    let Some((key, value)) = token.split_once('=') else {
        return if pattern_value_is_variable(token) {
            "<_>".to_string()
        } else {
            token.to_string()
        };
    };
    if key.is_empty() || value.is_empty() {
        return token.to_string();
    }
    if pattern_value_is_variable(value) {
        format!("{key}=<_>")
    } else {
        token.to_string()
    }
}
