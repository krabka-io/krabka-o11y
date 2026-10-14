use super::{Labels, Value};

pub(crate) fn json_object_to_labels(value: &Value) -> Option<Labels> {
    value.as_object().map(|object| {
        let mut labels = Labels::new();
        for entry in object {
            if let Some(value) = entry.1.as_str() {
                labels.insert(entry.0.clone(), value.to_string());
            }
        }
        labels
    })
}
