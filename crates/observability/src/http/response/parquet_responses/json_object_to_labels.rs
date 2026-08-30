use super::*;

pub(crate) fn json_object_to_labels(value: &Value) -> Option<Labels> {
    value.as_object().map(|object| {
        object
            .iter()
            .filter_map(|(name, value)| {
                value
                    .as_str()
                    .map(|value| (name.clone(), value.to_string()))
            })
            .collect()
    })
}
