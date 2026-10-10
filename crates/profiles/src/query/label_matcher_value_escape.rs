use super::{TabEscape, escape_quoted};

pub(crate) fn label_matcher_value_escape(value: &str) -> String {
    escape_quoted(value, TabEscape::Escaped)
}
