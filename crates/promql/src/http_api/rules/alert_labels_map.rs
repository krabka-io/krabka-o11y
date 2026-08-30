use super::*;

pub(crate) fn alert_labels_map(
    sample_labels: &Labels,
    rule: &serde_yaml::Value,
    alert_name: &str,
) -> BTreeMap<String, String> {
    let mut labels = sample_labels
        .iter()
        .filter(|(name, _)| name.as_str() != "__name__")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    labels.insert("alertname".to_string(), alert_name.to_string());
    if let Value::Object(rule_labels) = yaml_mapping_json(rule, "labels") {
        labels.extend(
            rule_labels
                .into_iter()
                .filter_map(|(name, value)| Some((name, value.as_str()?.to_string()))),
        );
    }
    labels
}
