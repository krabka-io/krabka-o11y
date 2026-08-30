//! Ruler evaluation helpers.

use std::collections::BTreeMap;

use krabka_metrics::WalRecord;

use crate::PromqlError;

mod alerting;
mod config;
mod evaluation;
mod recording;
mod schedule;

pub use alerting::{
    evaluate_and_dispatch_alerting_rule, evaluate_and_dispatch_alerting_rule_group,
    evaluate_and_dispatch_alerting_rule_with_state, evaluate_and_persist_alerting_rule_group,
    evaluate_and_persist_alerting_rule_with_state,
};
#[cfg(test)]
use config::parse_duration;
pub use evaluation::{
    evaluate_and_persist_ruler_rule_group, evaluate_and_persist_ruler_rule_set,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval, evaluate_ruler_rule_group,
    evaluate_ruler_rule_set,
};
pub use recording::{
    evaluate_and_append_recording_rule, evaluate_and_append_recording_rule_group,
    evaluate_recording_rule,
};
pub use schedule::{
    RulerShard, filter_ruler_rule_set_due_for_eval, filter_ruler_rule_set_for_shard,
    filter_ruler_rule_set_for_shard_due_for_eval,
};

#[cfg(test)]
mod tests;

// === split-modules: generated submodules ===
mod alert_state_key;
mod alertmanager_alert;
mod alertmanager_sink;
mod noop_ruler_state_sink;
mod promql_error;
mod recording_rule_wal_sink;
mod ruler_alert_state;
mod ruler_alert_state_record;
mod ruler_group_evaluation;
mod ruler_group_state;
mod ruler_group_state_key;
mod ruler_group_state_record;
mod ruler_state_sink;
mod ruler_wal_error;

use alert_state_key::AlertStateKey;
pub use alertmanager_alert::AlertmanagerAlert;
pub use alertmanager_sink::AlertmanagerSink;
use noop_ruler_state_sink::NoopRulerStateSink;
pub use recording_rule_wal_sink::RecordingRuleWalSink;
pub use ruler_alert_state::RulerAlertState;
pub use ruler_alert_state_record::RulerAlertStateRecord;
pub use ruler_group_evaluation::RulerGroupEvaluation;
pub use ruler_group_state::RulerGroupState;
use ruler_group_state_key::RulerGroupStateKey;
pub use ruler_group_state_record::RulerGroupStateRecord;
pub use ruler_state_sink::RulerStateSink;
pub use ruler_wal_error::RulerWalError;
