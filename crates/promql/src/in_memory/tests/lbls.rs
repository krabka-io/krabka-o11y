use super::*;

pub(crate) fn lbls(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (key, value) in pairs {
        labels.insert(*key, *value);
    }
    labels
}
