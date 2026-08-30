use super::{loki_rule_group_name, loki_yaml_mapping, serde_yaml_key, validate_loki_rule};

pub(crate) fn validate_loki_rule_group(rule_group: &serde_yaml::Value) -> Result<(), ()> {
    let fields = loki_yaml_mapping(rule_group).ok_or(())?;
    if loki_rule_group_name(rule_group).is_none() {
        return Err(());
    }
    let rules = fields
        .get(serde_yaml_key("rules"))
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or(())?;
    for rule in rules {
        validate_loki_rule(rule)?;
    }
    Ok(())
}
