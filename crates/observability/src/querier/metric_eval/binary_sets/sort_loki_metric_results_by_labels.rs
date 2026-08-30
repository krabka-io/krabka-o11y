use super::{Value, metric_series_labels};

pub(crate) fn sort_loki_metric_results_by_labels(results: &mut [Value]) {
    results.sort_by_key(metric_series_labels);
}
