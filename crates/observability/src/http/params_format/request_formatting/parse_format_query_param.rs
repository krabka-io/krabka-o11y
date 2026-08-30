use super::*;

pub(crate) fn parse_format_query_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::LokiFormatMissingQuery);
    };
    for pair in split_query_param_pairs(raw_query, &["query"]) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key)? == "query" {
            return decode_form_component(value);
        }
    }
    Err(HttpQueryError::LokiFormatMissingQuery)
}
