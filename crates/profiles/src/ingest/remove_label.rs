use super::*;

pub(crate) fn remove_label(labels: &mut Labels, target: &str) {
    let mut rebuilt = Labels::new();
    for (name, value) in labels.iter() {
        if name != target {
            rebuilt.insert(name, value);
        }
    }
    *labels = rebuilt;
}
