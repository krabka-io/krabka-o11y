use std::collections::BTreeMap;

use krabka_blockstore::Labels;
use krabka_metrics::{SamplePayload, WalRecord};

use super::{
    RecordingRuleWalSink,
    config::{yaml_optional_string, yaml_required_string, yaml_string_map},
};
use crate::{MetricStore, PromqlEngine, PromqlError, QueryResult, SampleValue};

// === split-modules: generated submodules ===
mod evaluate_and_append_recording_rule;
mod evaluate_and_append_recording_rule_group;
mod evaluate_recording_rule;
mod recording_labels;

pub use evaluate_and_append_recording_rule::evaluate_and_append_recording_rule;
pub use evaluate_and_append_recording_rule_group::evaluate_and_append_recording_rule_group;
pub use evaluate_recording_rule::evaluate_recording_rule;
use recording_labels::recording_labels;
