use super::{MetricVectorMatching, Value};
use crate::querier::metric_eval::for_each_loki_result_metric;

pub(crate) fn retain_metric_binary_on_labels(
    value: &mut Value,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(MetricVectorMatching::On {
        labels,
        group: None,
    }) = matching
    else {
        return;
    };
    for_each_loki_result_metric(value, |metric| {
        metric.retain(|label, _| labels.contains(label));
    });
}
