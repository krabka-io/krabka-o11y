use super::*;

pub(crate) fn validate_rule(rule: &serde_yaml::Value) -> Result<(), ApiError> {
    let has_record = yaml_optional_string(rule, "record").is_some();
    let has_alert = yaml_optional_string(rule, "alert").is_some();
    match (has_record, has_alert) {
        (true, true) | (false, false) => {
            return Err(ApiError::bad_data(
                "rule must contain exactly one of record or alert",
            ));
        }
        _ => {}
    }
    let expr = yaml_optional_string(rule, "expr")
        .filter(|expr| !expr.is_empty())
        .ok_or_else(|| ApiError::bad_data("rule must contain a non-empty expr"))?;
    parse_promql(&expr)
        .map(|_| ())
        .map_err(|error| ApiError::bad_data(format!("rule PromQL expression is invalid: {error}")))
}
