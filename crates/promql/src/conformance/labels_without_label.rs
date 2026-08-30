use super::*;

pub(crate) fn labels_without_label(labels: &Labels, drop: &str) -> Labels {
    let mut out = Labels::new();
    for (name, value) in labels.iter() {
        if name != drop {
            out.insert(name, value);
        }
    }
    out
}
