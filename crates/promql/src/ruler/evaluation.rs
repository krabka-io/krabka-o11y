use std::collections::BTreeMap;

use krabka_blockstore::TenantId;

use super::{
    AlertmanagerSink, RecordingRuleWalSink, RulerAlertState, RulerEvaluationReport,
    RulerGroupEvaluation, RulerGroupEvaluationStatus, RulerGroupState, RulerGroupStateRecord,
    RulerRuleEvaluationStatus, RulerShard, RulerStateSink,
    config::{yaml_optional_string, yaml_required_string, yaml_string_map},
    evaluate_and_append_recording_rule, evaluate_and_append_recording_rule_group,
    evaluate_and_dispatch_alerting_rule_group,
    evaluate_and_persist_alerting_rule_with_state_and_wal,
    filter_ruler_rule_set_for_shard_due_for_eval,
};
use crate::{MetricStore, PromqlEngine, PromqlError};

mod evaluate_and_persist_ruler_rule_group;
mod evaluate_and_persist_ruler_rule_set;
mod evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval;
mod evaluate_ruler_rule_group;
mod evaluate_ruler_rule_set;

pub use evaluate_and_persist_ruler_rule_group::evaluate_and_persist_ruler_rule_group;
pub use evaluate_and_persist_ruler_rule_set::{
    evaluate_and_persist_ruler_rule_set, evaluate_and_persist_ruler_rule_set_with_report,
};
pub use evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval::{
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval_with_report,
};
pub use evaluate_ruler_rule_group::evaluate_ruler_rule_group;
pub use evaluate_ruler_rule_set::evaluate_ruler_rule_set;
