use super::{Labels, loki_yaml_mapping, serde_yaml_key};

pub(crate) fn yaml_string_labels_field(fields: &serde_yaml::Mapping, name: &'static str) -> Labels {
    fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            let mut labels = Labels::new();
            for entry in values {
                let Some(key) = entry.0.as_str() else {
                    continue;
                };
                let Some(value) = entry.1.as_str() else {
                    continue;
                };
                labels.insert(key.to_string(), value.to_string());
            }
            labels
        })
        .unwrap_or_default()
}
