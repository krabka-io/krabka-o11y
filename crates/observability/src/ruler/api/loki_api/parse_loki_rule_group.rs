use super::validate_loki_rule_group;

pub(crate) fn parse_loki_rule_group(body: &[u8]) -> Result<serde_yaml::Value, ()> {
    let rule_group = serde_yaml::from_slice(body).map_err(|_| ())?;
    validate_loki_rule_group(&rule_group)?;
    Ok(rule_group)
}
