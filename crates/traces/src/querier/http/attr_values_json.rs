use super::{AttrValue, Value, attr_value_json, json};

pub(crate) fn attr_values_json(values: &[&AttrValue]) -> Value {
    if let [value] = values {
        return attr_value_json(value);
    }
    json!({
        "arrayValue": {
            "values": values.iter().map(|value| attr_value_json(value)).collect::<Vec<_>>(),
        }
    })
}
