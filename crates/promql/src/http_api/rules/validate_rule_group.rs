use super::*;

pub(crate) fn validate_rule_group(group: &serde_yaml::Value) -> Result<(), ApiError> {
    let rules = group
        .get("rules")
        .and_then(serde_yaml::Value::as_sequence)
        .filter(|rules| !rules.is_empty())
        .ok_or_else(|| ApiError::bad_data("rule group YAML must contain at least one rule"))?;
    for rule in rules {
        validate_rule(rule)?;
    }
    Ok(())
}
