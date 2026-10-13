use super::Value;

pub(crate) fn labels_map_json(labels: crate::PromqlLabels) -> Value {
    let mut values = super::Map::new();
    for pair in labels.iter() {
        values.insert(
            pair.name.clone(),
            Value::String(pair.value.as_str().to_owned()),
        );
    }
    Value::Object(values)
}
