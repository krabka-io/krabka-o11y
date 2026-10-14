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

pub(crate) fn http_requests_total_api_labels() -> krabka_metrics::MetricLabels {
    krabka_metrics::MetricLabels::from_pairs([
        krabka_metrics::MetricLabelPair::new("__name__", "http_requests_total"),
        krabka_metrics::MetricLabelPair::new("job", "api"),
    ])
}
