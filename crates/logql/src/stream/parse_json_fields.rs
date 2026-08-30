use super::{Labels, insert_json_parser_error, flatten_json_field, sanitize_json_field_name};

pub(crate) fn parse_json_fields(line: &str, fields: &mut Labels) {
    let Ok(serde_json::Value::Object(object)) = serde_json::from_str(line) else {
        insert_json_parser_error(fields);
        return;
    };

    for (name, value) in object {
        flatten_json_field(&sanitize_json_field_name(&name), &value, fields);
    }
}
