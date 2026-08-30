use super::*;

pub(crate) fn label_join_format_query_error(query: &str) -> Option<String> {
    query
        .trim_start()
        .starts_with("label_join")
        .then(|| "parse error at line 1, col 1: syntax error: unexpected IDENTIFIER".to_string())
}
