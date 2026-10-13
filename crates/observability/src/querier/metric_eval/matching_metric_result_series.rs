use super::binary_sets::{metric_series_labels, metric_vector_matching_key};
use crate::{MetricVectorMatching, Value};

/// Keeps left-hand result series accepted by `keep`, paired with a matching
/// right-hand series when one exists.
pub(crate) fn retain_matching_metric_result_series(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    matching: Option<&MetricVectorMatching>,
    mut keep: impl FnMut(&mut Value, Option<&Value>) -> bool,
) {
    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let right_series = right_results.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == left_key)
        });

        if keep(&mut left_results[index], right_series) {
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}
