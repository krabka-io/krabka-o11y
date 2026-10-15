use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::MatchOp;
use promql_parser::parser::{Expr, LabelModifier, VectorSelector};

use super::selector::label_matcher_sets;
use crate::{
    PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher,
    error::Result,
    result::{InstantSample, SampleValue},
};

mod absent_labels;
mod absent_labels_from_matchers;

pub(super) use absent_labels::absent_labels;
use absent_labels_from_matchers::absent_labels_from_matchers;

pub(crate) fn absent_labels_from_selector(selector: &VectorSelector) -> Labels {
    let matcher_sets = label_matcher_sets(selector);
    if matcher_sets.len() == 1 {
        return absent_labels_from_matchers(&matcher_sets[0]);
    }
    Labels::new()
}

pub(crate) fn float_sample_value(sample: &InstantSample) -> Result<f64> {
    match sample.value {
        SampleValue::Float(value) => Ok(value),
        SampleValue::Histogram(_) => Err(PromqlError::Plan(
            "this evaluation path requires a float sample".to_string(),
        )),
    }
}

pub(crate) fn is_result_metadata_label(name: &str) -> bool {
    matches!(name, "__name__" | "__type__" | "__unit__")
}

pub(crate) fn labels_key(labels: &Labels) -> String {
    labels.order_key()
}

pub(crate) fn aggregate_labels(input: &Labels, modifier: Option<&LabelModifier>) -> Labels {
    let mut labels = Labels::new();
    match modifier {
        Some(LabelModifier::Include(include)) => {
            for name in &include.labels {
                if let Some(value) = input.get_value(name) {
                    labels.insert(name, value);
                }
            }
        }
        Some(LabelModifier::Exclude(exclude)) => {
            let excluded = exclude.labels.iter().collect::<BTreeSet<_>>();
            for pair in input.iter() {
                if pair.name == "__name__" || excluded.contains(pair.name) {
                    continue;
                }
                labels.insert(pair.name, pair.value);
            }
        }
        None => {}
    }
    labels
}

/// Returns `labels` without `drop`, keeping `__name__`.
///
/// This is `Labels.BytesWithoutLabels(le)`, the signature `resetHistograms`
/// groups classic buckets by.
pub(crate) fn labels_without_label(labels: &Labels, drop: &str) -> Labels {
    labels
        .iter()
        .filter(|pair| pair.name.as_str() != drop)
        .map(|pair| krabka_metrics::MetricLabelPair::new(pair.name.clone(), pair.value.clone()))
        .collect()
}

pub(crate) fn labels_without_metric_name(input: &Labels) -> Labels {
    let mut labels = Labels::new();
    for pair in input.iter() {
        // `__name__` is removed only when each operator's Prometheus semantics
        // require it; classic histogram grouping still needs the source name.
        if !matches!(pair.name.as_str(), "__type__" | "__unit__") {
            labels.insert(pair.name, pair.value);
        }
    }
    labels
}

/// Records the `__name__` of the first sample for a histogram group key.
///
/// A later mixed-histogram warning names the metric with this value, as
/// Prometheus does.
pub(crate) fn record_metric_name(names: &mut BTreeMap<String, String>, key: &str, labels: &Labels) {
    if let Some(name) = labels.get("__name__") {
        names
            .entry(key.to_string())
            .or_insert_with(|| name.to_string());
    }
}
