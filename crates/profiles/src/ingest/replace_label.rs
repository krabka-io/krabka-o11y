use super::Labels;

pub(crate) fn replace_label(labels: &mut Labels, target: &str, replacement: &str) {
    let mut rebuilt = super::labels_without_target(labels, target);
    rebuilt.insert(target, replacement);
    *labels = rebuilt;
}
