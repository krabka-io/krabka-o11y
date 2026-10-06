use super::Labels;

/// Returns the canonical `name=value\n…` rendering of a label set.
///
/// This rendering is the sort tiebreak and the collision key. It matches the
/// interpreter's `labels_key`.
pub(crate) fn labels_key(labels: &Labels) -> String {
    labels.order_key()
}
