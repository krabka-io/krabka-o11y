use super::{Labels, field_value_to_string, insert_extracted_field, sanitize_json_field_name};

pub(crate) fn flatten_json_field(name: &str, value: &serde_json::Value, fields: &mut Labels) {
    match value {
        serde_json::Value::Object(object) => {
            for (child_name, child_value) in object {
                let child_name = sanitize_json_field_name(child_name);
                let flattened_name = if name.is_empty() {
                    child_name
                } else {
                    format!("{name}_{child_name}")
                };
                flatten_json_field(&flattened_name, child_value, fields);
            }
        }
        serde_json::Value::Array(_) => {}
        _ => {
            insert_extracted_field(fields, name, field_value_to_string(value));
        }
    }
}
