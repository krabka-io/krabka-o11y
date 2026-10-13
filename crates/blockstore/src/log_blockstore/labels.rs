use super::*;

pub type Labels = BTreeMap<String, String>;

#[must_use]
pub fn labels<const N: usize>(items: [crate::labels::LabelPair; N]) -> Labels {
    let mut labels = Labels::new();
    for pair in items {
        labels.insert(pair.name, pair.value);
    }
    labels
}
