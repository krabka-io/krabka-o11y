use super::Value;

pub(crate) fn labels_map_json(labels: crate::PromqlLabels) -> Value {
    Value::Object(
        labels
            .into_iter()
            .map(|(name, value)| (name, Value::String(value.as_str().to_owned())))
            .collect(),
    )
}
