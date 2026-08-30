use super::*;

/// Builds a [`Labels`] set from an alert label map for template `$labels.NAME`
/// lookups.
pub(crate) fn labels_from_map(map: &BTreeMap<String, String>) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in map {
        labels.insert(name, value);
    }
    labels
}
