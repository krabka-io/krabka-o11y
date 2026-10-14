use super::{
    BTreeMap, DetectedFieldStats, Value, add_detected_field, detected_json_value_string,
    field_type_from_json,
};

pub(crate) fn detect_json_fields(fields: &mut BTreeMap<String, DetectedFieldStats>, line: &str) {
    let Ok(Value::Object(object)) = serde_json::from_str::<Value>(line) else {
        return;
    };
    for entry in object {
        let name = entry.0;
        let json_value = entry.1;
        let Some(value) = detected_json_value_string(&json_value) else {
            continue;
        };
        add_detected_field(
            fields,
            &name,
            value,
            field_type_from_json(&json_value),
            "json",
        );
    }
}
