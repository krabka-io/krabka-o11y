use super::Labels;

pub(crate) fn remove_label(labels: &mut Labels, target: &str) {
    *labels = super::labels_without_target(labels, target);
}
