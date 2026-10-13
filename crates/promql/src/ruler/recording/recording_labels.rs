use super::{BTreeMap, Labels};

pub(crate) fn recording_labels(
    mut labels: Labels,
    record_name: &str,
    rule_labels: &BTreeMap<String, String>,
) -> Labels {
    labels.insert("__name__", record_name);
    // Rule-level labels are applied on top of the series labels (rule labels
    // win), matching Prometheus recording-rule label semantics.
    for entry in rule_labels {
        let name = entry.0;
        let value = entry.1;
        labels.insert(name.clone(), value.clone());
    }
    labels
}
