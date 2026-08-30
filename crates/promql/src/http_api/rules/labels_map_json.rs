use super::{BTreeMap, Value};

pub(crate) fn labels_map_json(labels: BTreeMap<String, String>) -> Value {
    Value::Object(
        labels
            .into_iter()
            .map(|(name, value)| (name, Value::String(value)))
            .collect(),
    )
}
