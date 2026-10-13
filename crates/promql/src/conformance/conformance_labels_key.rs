use super::Labels;

pub(crate) fn conformance_labels_key(labels: &Labels) -> String {
    let mut key = String::new();
    for pair in labels.iter() {
        key.push_str(pair.name);
        key.push('=');
        key.push_str(pair.value);
        key.push('\n');
    }
    key
}
