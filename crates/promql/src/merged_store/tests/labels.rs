use super::*;

pub(crate) fn labels(pairs: &[(&str, &str)]) -> Labels {
    let mut labels = Labels::new();
    for (name, value) in pairs {
        labels.insert(*name, *value);
    }
    labels
}
