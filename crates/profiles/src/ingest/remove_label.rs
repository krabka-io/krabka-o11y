use super::Labels;

pub(crate) fn remove_label(labels: &mut Labels, target: &str) {
    let mut rebuilt = Labels::new();
    for pair in labels.iter() {
        let name = pair.name;
        let value = pair.value;
        if name != target {
            rebuilt.insert(name, value);
        }
    }
    *labels = rebuilt;
}
