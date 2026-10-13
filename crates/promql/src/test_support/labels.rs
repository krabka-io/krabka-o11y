use krabka_blockstore::Labels;

use super::TestLabelPair;

pub(crate) fn labels(pairs: &[TestLabelPair<'_>]) -> Labels {
    let mut labels = Labels::new();
    for pair in pairs {
        labels.insert(pair.name, pair.value);
    }
    labels
}

pub(crate) fn up_instance_labels(instance: &str) -> Labels {
    labels(&[
        TestLabelPair::new("__name__", "up"),
        TestLabelPair::new("instance", instance),
        TestLabelPair::new("job", "api"),
    ])
}
