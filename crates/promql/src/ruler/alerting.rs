use std::collections::BTreeMap;

use krabka_blockstore::TenantId;
use krabka_metrics::SamplePayload;
use krabka_units::prelude::*;

use super::{
    AlertStateKey, AlertmanagerAlert, AlertmanagerSink, NoopRecordingRuleWalSink,
    NoopRulerStateSink, RecordingRuleWalSink, RulerAlertState, RulerAlertStateRecord,
    RulerStateSink, WalRecord,
    config::{yaml_duration, yaml_optional_string, yaml_required_string, yaml_string_map},
};
use crate::{MetricStore, PromqlEngine, PromqlError, QueryResult, SampleValue};

#[cfg(test)]
mod alert_template_queries;
mod evaluate_alerting_rule_with_state_and_sink;
mod evaluate_and_dispatch_alerting_rule;
mod evaluate_and_dispatch_alerting_rule_group;
mod evaluate_and_dispatch_alerting_rule_with_state;
mod evaluate_and_persist_alerting_rule_group;
mod evaluate_and_persist_alerting_rule_with_state;
mod expand_alert_label_map;
mod labels_to_map;
mod template_query_value;

pub(crate) use evaluate_alerting_rule_with_state_and_sink::evaluate_alerting_rule_with_state_and_sink;
pub use evaluate_and_dispatch_alerting_rule::evaluate_and_dispatch_alerting_rule;
pub use evaluate_and_dispatch_alerting_rule_group::evaluate_and_dispatch_alerting_rule_group;
pub use evaluate_and_dispatch_alerting_rule_with_state::evaluate_and_dispatch_alerting_rule_with_state;
pub use evaluate_and_persist_alerting_rule_group::evaluate_and_persist_alerting_rule_group;
pub use evaluate_and_persist_alerting_rule_with_state::evaluate_and_persist_alerting_rule_with_state;
#[cfg(test)]
pub(crate) use expand_alert_label_map::expand_alert_label_map;
pub(crate) use expand_alert_label_map::{alert_template_variables, expand_alert_label_map_async};
use labels_to_map::labels_to_map;
pub(crate) use template_query_value::{template_query_value, template_sample_value};
