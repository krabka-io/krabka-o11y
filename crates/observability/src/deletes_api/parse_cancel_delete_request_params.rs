use super::{DecodedQueryPair, HttpQueryError, decode_query_pair};

pub(crate) fn parse_cancel_delete_request_params(
    raw_query: Option<&str>,
) -> Result<String, HttpQueryError> {
    let mut request_id = None;
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("request_id"));
    };
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let DecodedQueryPair { key, value } = decode_query_pair(pair)?;
        match key.as_str() {
            "request_id" => request_id = Some(value),
            "force" => match value.as_str() {
                "true" | "false" => {}
                _ => {
                    return Err(HttpQueryError::InvalidQueryParameter {
                        name: "force",
                        value,
                    });
                }
            },
            _ => {}
        }
    }
    request_id.ok_or(HttpQueryError::MissingQueryParameter("request_id"))
}
