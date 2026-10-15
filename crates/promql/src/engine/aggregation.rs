use std::{cmp::Ordering, collections::BTreeMap};

use krabka_metrics::{NativeHistogram, ResetHint};
use promql_parser::parser::{
    LabelModifier,
    token::{T_TOPK, TokenType},
};

use super::{
    annotations::{
        emit_info, emit_warning, histogram_counter_reset_collision_warning,
        histogram_ignored_in_aggregation_info, invalid_quantile_warning, is_valid_quantile,
        mismatched_custom_buckets_info, mixed_exponential_custom_warning,
        mixed_floats_histograms_agg_warning,
    },
    histogram::HistogramAccumulator,
    labels::{aggregate_labels, float_sample_value, labels_key},
    range_functions::kahan_sum_inc,
    result_utils::quantile_value,
};
use crate::{
    PromqlLabels as Labels,
    error::{PromqlError, Result},
    result::{InstantSample, SampleValue},
};

#[cfg(all(test, feature = "experimental-functions"))]
mod tests {
    use assert2::assert;
    use num_traits::ToPrimitive as _;

    use super::*;

    #[test]
    fn limit_ratio_uses_a_strict_positive_hash_threshold() {
        let mut labels = Labels::new();
        labels.insert("__name__", "requests_total");
        labels.insert("instance", "api-1");
        let offset = prometheus_labels_hash(&labels).to_f64().unwrap() / u64::MAX.to_f64().unwrap();

        assert!(!limit_ratio_includes_sample(offset, &labels));
        assert!(limit_ratio_includes_sample(offset.next_up(), &labels));
        assert!(!limit_ratio_includes_sample(0.0, &labels));
        assert!(!limit_ratio_includes_sample(-0.0, &labels));
    }
}

mod aggregate_op;
mod aggregate_state;
mod apply_count_values_aggregate;
mod apply_k_aggregate;
mod apply_limitk_aggregate;
mod apply_quantile_aggregate;
mod apply_simple_aggregate;
mod apply_stddev_stdvar_aggregate;
mod compare_k_aggregate_samples;
mod counter_reset_hints;
mod limit_ratio_includes_sample;

pub(super) use aggregate_op::AggregateOp;
pub(super) use aggregate_state::AggregateState;
pub(super) use apply_count_values_aggregate::apply_count_values_aggregate;
pub(super) use apply_k_aggregate::apply_k_aggregate;
#[cfg(feature = "experimental-functions")]
#[cfg(feature = "experimental-functions")]
pub(super) use apply_limitk_aggregate::apply_limitk_aggregate;
pub(super) use apply_quantile_aggregate::apply_quantile_aggregate;
pub(super) use apply_simple_aggregate::apply_simple_aggregate;
pub(super) use apply_stddev_stdvar_aggregate::apply_stddev_stdvar_aggregate;
use compare_k_aggregate_samples::compare_k_aggregate_samples;
use counter_reset_hints::CounterResetHints;
#[cfg(feature = "experimental-functions")]
use limit_ratio_includes_sample::limit_ratio_includes_sample;
pub(crate) fn count_values_label_value(value: &SampleValue) -> String {
    match value {
        // Render the float with the crate's canonical Prometheus formatter so
        // non-finite values match the wire form (`+Inf`/`-Inf`/`NaN`) rather than
        // `f64::to_string`'s `inf`/`-inf`/`NaN`.
        SampleValue::Float(value) => crate::http_api::format_sample_value(*value),
        SampleValue::Histogram(histogram) => crate::http_api::native_histogram_string(histogram),
    }
}

#[cfg(feature = "experimental-functions")]
/// Shared experimental `limit_ratio(ratio, v)` core over an already-evaluated
/// instant vector.
///
/// This function backs both the interpreter
/// (`PromqlEngine::eval_limit_ratio_aggregate`) and the operator path. It keeps
/// each sample whose label-set hash falls in the ratio's deterministic selection
/// band, as [`limit_ratio_includes_sample`] defines. The caller resolves and
/// caps the ratio before reaching here, and raises the `InvalidRatioWarning`
/// when the ratio was out of range. The caller also short-circuits `ratio==0` to
/// the empty vector.
#[cfg(feature = "experimental-functions")]
pub(crate) fn apply_limit_ratio_aggregate(
    samples: Vec<InstantSample>,
    ratio: f64,
) -> Vec<InstantSample> {
    samples
        .into_iter()
        .filter(|sample| limit_ratio_includes_sample(ratio, &sample.labels))
        .collect()
}

#[cfg(feature = "experimental-functions")]
/// Hashes labels exactly like Prometheus' `labels.Labels.Hash`.
///
/// Krabka's persisted series fingerprint deliberately uses a different,
/// length-prefixed encoding. `PromQL`'s `limit_ratio`, however, is externally
/// observable and must use Prometheus' xxHash64 over sorted
/// `name\xffvalue\xff` pairs.
#[cfg(feature = "experimental-functions")]
pub(crate) fn prometheus_labels_hash(labels: &Labels) -> u64 {
    let capacity = labels
        .iter()
        .map(|pair| pair.name.len() + pair.value.len() + 2)
        .sum();
    let mut bytes = Vec::with_capacity(capacity);
    for pair in labels.iter() {
        bytes.extend_from_slice(pair.name.as_bytes());
        bytes.push(0xff);
        bytes.extend_from_slice(pair.value.as_bytes());
        bytes.push(0xff);
    }
    xxhash_rust::xxh64::xxh64(&bytes, 0)
}
