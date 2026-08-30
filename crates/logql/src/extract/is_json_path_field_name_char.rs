use super::*;

pub(crate) fn is_json_path_field_name_char(ch: char) -> bool {
    matches!(ch, '_' | ':' | '-') || ch.is_ascii_alphanumeric()
}
