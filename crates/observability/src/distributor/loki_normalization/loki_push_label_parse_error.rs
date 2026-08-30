use super::{Labels, is_loki_label_name_char, loki_label_set};

pub(crate) fn loki_push_label_parse_error(labels: &Labels, invalid_name: &str) -> String {
    let rendered = loki_label_set(labels);
    let name_start = rendered.find(invalid_name).unwrap_or(1);
    let invalid_offset = invalid_name
        .char_indices()
        .find_map(|(offset, value)| {
            (!is_loki_label_name_char(value, offset == 0)).then_some(offset)
        })
        .unwrap_or(0);
    let column = name_start + invalid_offset + 1;
    let unexpected = invalid_name[invalid_offset..].chars().next().unwrap_or('}');
    format!(
        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{unexpected}'\n"
    )
}
