use super::*;

pub(crate) fn is_loki_label_name_char(value: char, first: bool) -> bool {
    value == '_' || value.is_ascii_alphabetic() || (!first && value.is_ascii_digit())
}
