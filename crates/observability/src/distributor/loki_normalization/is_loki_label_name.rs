use super::*;

pub(crate) fn is_loki_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    is_loki_label_name_char(first, true) && chars.all(|value| is_loki_label_name_char(value, false))
}
