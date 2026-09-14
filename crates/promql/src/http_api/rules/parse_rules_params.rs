use super::{ApiError, RulesParams, form_urlencoded};

pub(crate) fn parse_rules_params(raw_query: Option<&str>) -> Result<RulesParams, ApiError> {
    let mut params = RulesParams::default();
    let mut bracket_rule_names = Vec::new();
    let mut bracket_rule_groups = Vec::new();
    let mut bracket_files = Vec::new();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "type" => match value.to_ascii_lowercase().as_str() {
                kind @ ("alert" | "record") => params.rule_type = Some(kind.to_string()),
                _ => {
                    return Err(ApiError::bad_data(format!(
                        "not supported value {:?}",
                        value.as_ref()
                    )));
                }
            },
            "exclude_alerts" => {
                params.exclude_alerts = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid exclude_alerts parameter"))?,
                );
            }
            "rule_name" if !value.is_empty() => {
                params.rule_names.insert(value.into_owned());
            }
            "rule_name[]" if !value.is_empty() => bracket_rule_names.push(value.into_owned()),
            "rule_group" if !value.is_empty() => {
                params.rule_groups.insert(value.into_owned());
            }
            "rule_group[]" if !value.is_empty() => bracket_rule_groups.push(value.into_owned()),
            "file" if !value.is_empty() => {
                params.files.insert(value.into_owned());
            }
            "file[]" if !value.is_empty() => bracket_files.push(value.into_owned()),
            "group_limit" if !value.is_empty() => {
                let limit = value
                    .parse::<i32>()
                    .map_err(|_| ApiError::bad_data("invalid group limit value"))?;
                if limit < 0 {
                    return Err(ApiError::bad_data("invalid group limit value"));
                }
                params.group_limit = usize::try_from(limit).ok();
            }
            "group_next_token" => params.group_next_token = Some(value.into_owned()),
            _ => {}
        }
    }
    if !bracket_rule_names.is_empty() {
        params.rule_names = bracket_rule_names.into_iter().collect();
    }
    if !bracket_rule_groups.is_empty() {
        params.rule_groups = bracket_rule_groups.into_iter().collect();
    }
    if !bracket_files.is_empty() {
        params.files = bracket_files.into_iter().collect();
    }
    Ok(params)
}
