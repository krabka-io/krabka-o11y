use super::*;

#[derive(Debug, Deserialize)]
pub(crate) struct ParseQueryParams {
    pub(crate) query: String,
}

pub(crate) fn parse_query_params(body: &[u8]) -> Result<ParseQueryParams, ApiError> {
    let mut query = None;
    for (name, value) in form_urlencoded::parse(body) {
        if name == "query" {
            query = Some(value.into_owned());
        }
    }
    Ok(ParseQueryParams {
        query: required_form_param(query, "query")?,
    })
}
