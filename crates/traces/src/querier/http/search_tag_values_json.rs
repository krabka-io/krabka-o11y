use super::{json, TypedValue, Value};

pub(crate) fn search_tag_values_json(values: &[TypedValue]) -> Value {
    json!({
        "tagValues": values.iter().map(|value| &value.value).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}
