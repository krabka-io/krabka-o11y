use super::*;

pub(crate) fn sql_string_literal(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}
