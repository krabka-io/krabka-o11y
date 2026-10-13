use super::{HttpQueryError, decode_query_pair, split_query_param_pairs};

pub(crate) fn parse_format_query_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::LokiFormatMissingQuery);
    };
    for pair in split_query_param_pairs(raw_query, &["query"]) {
        let pair = decode_query_pair(pair)?;
        if pair.key == "query" {
            return Ok(pair.value);
        }
    }
    Err(HttpQueryError::LokiFormatMissingQuery)
}
