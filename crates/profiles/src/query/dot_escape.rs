use super::{TabEscape, escape_quoted};

pub(crate) fn dot_escape(value: &str) -> String {
    escape_quoted(value, TabEscape::Literal)
}
