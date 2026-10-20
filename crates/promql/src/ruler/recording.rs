use std::collections::BTreeMap;

use krabka_blockstore::TenantId;
use krabka_metrics::{SamplePayload, WalRecord};

use super::{
    RecordingRuleWalSink,
    config::{yaml_optional_string, yaml_required_string, yaml_string_map},
};
use crate::{
    MetricStore, PromqlEngine, PromqlError, PromqlLabels as Labels, QueryResult, SampleValue,
};

pub(crate) fn recording_labels(
    mut labels: Labels,
    record_name: &str,
    rule_labels: &BTreeMap<String, String>,
) -> Labels {
    labels.insert("__name__", record_name);
    // Rule-level labels are applied on top of the series labels (rule labels
    // win), matching Prometheus recording-rule label semantics.
    for entry in rule_labels {
        let name = entry.0;
        let value = entry.1;
        labels.insert(name.clone(), value.clone());
    }
    labels
}

mod operations;
pub use operations::{
    evaluate_and_append_recording_rule, evaluate_and_append_recording_rule_group,
    evaluate_recording_rule,
};
