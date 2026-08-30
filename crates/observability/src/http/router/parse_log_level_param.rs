use super::*;

pub(crate) fn parse_log_level_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("log_level"));
    };
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key)? == "log_level" {
            let level = decode_form_component(value)?;
            return match level.as_str() {
                "debug" | "info" | "warn" | "error" => Ok(level),
                _ => Err(HttpQueryError::InvalidQueryParameter {
                    name: "log_level",
                    value: level,
                }),
            };
        }
    }
    Err(HttpQueryError::MissingQueryParameter("log_level"))
}
