use std::collections::BTreeMap;

use krabka_blockstore::Labels;
use krabka_units::prelude::*;

use super::{
    AlertStateKey, AlertmanagerAlert, AlertmanagerSink, NoopRulerStateSink, RulerAlertState,
    RulerAlertStateRecord, RulerStateSink,
    config::{yaml_duration, yaml_optional_string, yaml_required_string, yaml_string_map},
};
use crate::{MetricStore, PromqlEngine, PromqlError, QueryResult, SampleValue};

mod evaluate_alerting_rule_with_state_and_sink;
mod evaluate_and_dispatch_alerting_rule;
mod evaluate_and_dispatch_alerting_rule_group;
mod evaluate_and_dispatch_alerting_rule_with_state;
mod evaluate_and_persist_alerting_rule_group;
mod evaluate_and_persist_alerting_rule_with_state;
mod expand_alert_label_map;
mod labels_to_map;

use evaluate_alerting_rule_with_state_and_sink::evaluate_alerting_rule_with_state_and_sink;
pub use evaluate_and_dispatch_alerting_rule::evaluate_and_dispatch_alerting_rule;
pub use evaluate_and_dispatch_alerting_rule_group::evaluate_and_dispatch_alerting_rule_group;
pub use evaluate_and_dispatch_alerting_rule_with_state::evaluate_and_dispatch_alerting_rule_with_state;
pub use evaluate_and_persist_alerting_rule_group::evaluate_and_persist_alerting_rule_group;
pub use evaluate_and_persist_alerting_rule_with_state::evaluate_and_persist_alerting_rule_with_state;
use expand_alert_label_map::expand_alert_label_map;
use labels_to_map::labels_to_map;
