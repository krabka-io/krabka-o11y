use super::Labels;

pub(crate) fn labels_without_label(labels: &Labels, drop: &str) -> Labels {
    let mut out = Labels::new();
    for pair in labels.iter() {
        if pair.name != drop {
            out.insert(pair.name, pair.value);
        }
    }
    out
}
