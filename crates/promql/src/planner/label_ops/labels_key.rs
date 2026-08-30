use super::*;

/// Returns the canonical `name=value\n…` rendering of a label set.
///
/// This rendering is the sort tiebreak and the collision key. It matches the
/// interpreter's `labels_key`.
pub(crate) fn labels_key(labels: &Labels) -> String {
    let mut key = String::new();
    for (name, value) in labels.iter() {
        key.push_str(name);
        key.push('=');
        key.push_str(value);
        key.push('\n');
    }
    key
}
