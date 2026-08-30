use super::*;

pub(crate) fn loki_proto_label_parse_error(labels: &str) -> Option<String> {
    let labels = labels.trim();
    let mut chars = labels.char_indices();
    if chars.next()? != (0, '{') {
        return None;
    }

    let mut in_string = false;
    let mut escaped = false;
    let mut expecting_name = true;
    let mut first_name_char = true;

    for (offset, value) in chars {
        if in_string {
            if escaped {
                escaped = false;
            } else if value == '\\' {
                escaped = true;
            } else if value == '"' {
                in_string = false;
            }
            continue;
        }

        match value {
            '"' => in_string = true,
            ',' => {
                expecting_name = true;
                first_name_char = true;
            }
            // No `first_name_char` here: nothing reads it until a `,` starts
            // the next name, and that arm sets it itself.
            '=' => expecting_name = false,
            '}' => break,
            value if expecting_name && value.is_whitespace() => {}
            value if expecting_name => {
                if !is_loki_label_name_char(value, first_name_char) {
                    let column = labels[..offset].chars().count() + 1;
                    return Some(format!(
                        "couldn't parse labels: 1:{column}: parse error: unexpected character inside braces: '{value}'\n"
                    ));
                }
                first_name_char = false;
            }
            _ => {}
        }
    }

    None
}
