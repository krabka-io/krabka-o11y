use std::collections::BTreeMap;

use krabka_blockstore::{Labels, TenantId};
use krabka_metrics::SamplePayload;
use krabka_units::prelude::*;

use super::{
    AlertStateKey, AlertmanagerAlert, AlertmanagerSink, NoopRecordingRuleWalSink,
    NoopRulerStateSink, RecordingRuleWalSink, RulerAlertState, RulerAlertStateRecord,
    RulerStateSink, WalRecord,
    config::{yaml_duration, yaml_optional_string, yaml_required_string, yaml_string_map},
};
use crate::{MetricStore, PromqlEngine, PromqlError, QueryResult, SampleValue};

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

use alert_template_queries::alert_template_queries;
use evaluate_alerting_rule_with_state_and_sink::evaluate_alerting_rule_with_state_and_sink;
pub(crate) use evaluate_alerting_rule_with_state_and_sink::evaluate_and_persist_alerting_rule_with_state_and_wal;
pub use evaluate_and_dispatch_alerting_rule::evaluate_and_dispatch_alerting_rule;
pub use evaluate_and_dispatch_alerting_rule_group::evaluate_and_dispatch_alerting_rule_group;
pub use evaluate_and_dispatch_alerting_rule_with_state::evaluate_and_dispatch_alerting_rule_with_state;
pub use evaluate_and_persist_alerting_rule_group::evaluate_and_persist_alerting_rule_group;
pub use evaluate_and_persist_alerting_rule_with_state::evaluate_and_persist_alerting_rule_with_state;
use expand_alert_label_map::expand_alert_label_map;
use labels_to_map::labels_to_map;
use template_query_value::template_query_value;
