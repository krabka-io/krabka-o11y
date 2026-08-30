use super::*;

pub(crate) fn string_lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}
