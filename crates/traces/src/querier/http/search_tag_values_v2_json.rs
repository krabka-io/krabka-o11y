use super::{json, TypedValue, Value};

pub(crate) fn search_tag_values_v2_json(values: &[TypedValue]) -> Value {
    json!({
        "tagValues": values.iter().map(|value| {
            json!({
                "type": &value.type_,
                "value": &value.value,
            })
        }).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}
