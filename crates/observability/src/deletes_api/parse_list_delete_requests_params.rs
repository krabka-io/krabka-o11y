use super::{
    DecodedQueryPair, HttpQueryError, ListDeleteRequestsParams, decode_query_pair,
    parse_loki_delete_timestamp_query_param,
};

pub(crate) fn parse_list_delete_requests_params(
    raw_query: Option<&str>,
) -> Result<ListDeleteRequestsParams, HttpQueryError> {
    let mut start_time = None;
    let mut end_time = None;
    if let Some(raw_query) = raw_query {
        for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
            let DecodedQueryPair { key, value } = decode_query_pair(pair)?;
            match key.as_str() {
                "start" => {
                    start_time = Some(parse_loki_delete_timestamp_query_param("start", &value)?);
                }
                "end" => end_time = Some(parse_loki_delete_timestamp_query_param("end", &value)?),
                _ => {}
            }
        }
    }
    if start_time.is_some() != end_time.is_some() {
        return Err(HttpQueryError::InvalidQueryParameter {
            name: "start",
            value: "start and end must be provided together".to_string(),
        });
    }
    Ok(ListDeleteRequestsParams {
        start_time,
        end_time,
    })
}
