use super::sql_string_literal;

pub(crate) fn sql_like_pattern_literal(value: &str) -> String {
    sql_string_literal(value)
        .replace('%', "\\%")
        .replace('_', "\\_")
}
