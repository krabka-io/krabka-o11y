use std::collections::BTreeMap;

use krabka_blockstore::TenantId;

use super::{
    AlertmanagerSink, RecordingRuleWalSink, RulerAlertState, RulerEvaluationReport,
    RulerEvaluationSchedule, RulerGroupEvaluation, RulerGroupEvaluationStatus,
    RulerGroupStateRecord, RulerRuleEvaluationStatus, RulerSinks, RulerStateSink,
    config::{yaml_optional_string, yaml_required_string, yaml_string_map},
    evaluate_alerting_rule_with_state_and_sink, evaluate_and_append_recording_rule,
    evaluate_and_append_recording_rule_group, evaluate_and_dispatch_alerting_rule_group,
    filter_ruler_rule_set_for_shard_due_for_eval,
};
use crate::{MetricStore, PromqlEngine, PromqlError};

pub(super) struct RulerGroupRef<'a> {
    pub(super) namespace: &'a str,
    pub(super) group_name: &'a str,
    pub(super) group: &'a serde_yaml::Value,
}

mod workflows;
pub use workflows::{
    RulerRuleSetEvaluationRequest, evaluate_and_persist_ruler_rule_group,
    evaluate_and_persist_ruler_rule_set,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report,
    evaluate_and_persist_ruler_rule_set_with_report, evaluate_ruler_rule_group,
    evaluate_ruler_rule_set,
};
