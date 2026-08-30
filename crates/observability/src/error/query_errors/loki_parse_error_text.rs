use super::*;

pub(crate) fn loki_parse_error_text(query: &str, source: &ParseError) -> String {
    match source {
        ParseError::Syntax { message, position } => {
            let unexpected = unexpected_logql_token(query, *position);
            let prefix = format!(
                "parse error at line {}, col {}: syntax error: unexpected {}",
                line_number(query, *position),
                column_number(query, *position),
                unexpected
            );
            if should_omit_expected_logql_token(message, &unexpected) {
                prefix
            } else {
                format!("{prefix}, expecting {}", expected_logql_token(message))
            }
        }
        ParseError::InvalidRegex { pattern, source } => {
            format!("parse error: invalid regex `{pattern}`: {source}")
        }
    }
}
