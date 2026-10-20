use super::{ApiError, Deserialize, form_urlencoded, required_form_param};

#[derive(Debug, Deserialize)]
pub(crate) struct ParseQueryParams {
    pub(crate) query: String,
}

pub(crate) fn parse_query_params(body: &[u8]) -> Result<ParseQueryParams, ApiError> {
    let mut query = None;
    for pair in form_urlencoded::parse(body) {
        let name = pair.0;
        let value = pair.1;
        if name == "query" {
            query = Some(value.into_owned());
        }
    }
    Ok(ParseQueryParams {
        query: required_form_param(query, "query")?,
    })
}
