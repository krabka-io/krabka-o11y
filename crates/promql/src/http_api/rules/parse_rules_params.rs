use super::{RulesParams, ApiError, form_urlencoded};

pub(crate) fn parse_rules_params(raw_query: Option<&str>) -> Result<RulesParams, ApiError> {
    let mut params = RulesParams::default();
    let Some(raw_query) = raw_query else {
        return Ok(params);
    };
    for (name, value) in form_urlencoded::parse(raw_query.as_bytes()) {
        match name.as_ref() {
            "type" => match value.as_ref() {
                "alert" | "record" => params.rule_type = Some(value.into_owned()),
                _ => return Err(ApiError::bad_data("invalid type parameter")),
            },
            "exclude_alerts" => {
                params.exclude_alerts = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid exclude_alerts parameter"))?,
                );
            }
            _ => {}
        }
    }
    Ok(params)
}
