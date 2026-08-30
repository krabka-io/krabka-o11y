use super::*;

pub(crate) fn prometheus_rule_group_interval_seconds(group: &serde_yaml::Value) -> i64 {
    loki_yaml_mapping(group)
        .and_then(|fields| yaml_duration_seconds_field(fields, "interval"))
        .unwrap_or(0)
}
