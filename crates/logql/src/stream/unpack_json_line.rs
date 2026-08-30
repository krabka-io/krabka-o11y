use super::*;

pub(crate) fn unpack_json_line(line: &mut String, fields: &mut Labels) {
    let Ok(serde_json::Value::Object(object)) = serde_json::from_str(line) else {
        insert_json_parser_error(fields);
        return;
    };

    let mut replacement = None;
    for (name, value) in object {
        if name == "_entry" {
            if let serde_json::Value::String(entry) = value {
                replacement = Some(entry);
            }
            continue;
        }

        flatten_json_field(&sanitize_json_field_name(&name), &value, fields);
    }

    if let Some(entry) = replacement {
        *line = entry;
    }
}
