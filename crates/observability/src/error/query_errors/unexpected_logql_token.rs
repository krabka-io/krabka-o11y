use super::*;

pub(crate) fn unexpected_logql_token(query: &str, position: usize) -> String {
    let rest = &query[position.min(query.len())..];
    let Some(token) = rest.chars().next() else {
        return "$end".to_string();
    };
    if token == '_' || token.is_ascii_alphabetic() {
        return "IDENTIFIER".to_string();
    }
    token.to_string()
}
