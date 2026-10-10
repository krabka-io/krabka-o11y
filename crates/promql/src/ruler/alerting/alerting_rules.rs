use super::{PromqlError, yaml_optional_string};

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
