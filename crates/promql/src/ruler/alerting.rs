use std::collections::BTreeMap;

use krabka_blockstore::TenantId;
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
mod expand_alert_label_map;
mod template_query_value;
#[cfg(test)]
pub(crate) use expand_alert_label_map::expand_alert_label_map;
pub(crate) use expand_alert_label_map::{alert_template_variables, expand_alert_label_map_async};
pub(crate) use template_query_value::{template_query_value, template_sample_value};

pub(crate) fn labels_to_map(labels: &crate::PromqlLabels) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for pair in labels.iter() {
        values.insert(pair.name.clone(), pair.value.as_str().to_owned());
    }
    values
}

/// The alerting rules of one rule group, in order, without its recording
/// rules.
pub(crate) fn alerting_rules(
    group: &serde_yaml::Value,
) -> Result<impl Iterator<Item = &serde_yaml::Value>, PromqlError> {
    let Some(rules) = group.get("rules").and_then(serde_yaml::Value::as_sequence) else {
        return Err(PromqlError::Exec(
            "alerting rule group must contain rules".into(),
        ));
    };
    Ok(rules
        .iter()
        .filter(|rule| yaml_optional_string(rule, "alert").is_some()))
}

mod operations;
pub(crate) use operations::evaluate_alerting_rule_with_state_and_sink;
pub use operations::evaluate_and_dispatch_alerting_rule;
pub use operations::evaluate_and_dispatch_alerting_rule_with_state;
pub use operations::evaluate_and_dispatch_alerting_rule_group;
pub use operations::evaluate_and_persist_alerting_rule_with_state;
pub use operations::evaluate_and_persist_alerting_rule_group;
