use super::Labels;

pub(crate) fn replace_label(labels: &mut Labels, target: &str, replacement: &str) {
    let mut rebuilt = Labels::new();
    for (name, value) in labels.iter() {
        if name != target {
            rebuilt.insert(name, value);
        }
    }
    rebuilt.insert(target, replacement);
    *labels = rebuilt;
}
