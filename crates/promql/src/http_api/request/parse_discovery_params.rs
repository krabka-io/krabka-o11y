use super::{ApiError, DiscoveryParams};
use crate::http_api::parse_query_pairs;

pub(crate) fn parse_discovery_params(raw_query: Option<&str>) -> Result<DiscoveryParams, ApiError> {
    let mut params = DiscoveryParams::default();
    parse_query_pairs(raw_query, |name, value| {
        match name.as_str() {
            "match[]" => params.matches.push(value),
            "start" => params.start = Some(value),
            "end" => params.end = Some(value),
            "limit" => {
                params.limit = Some(
                    value
                        .parse()
                        .map_err(|_| ApiError::bad_data("invalid limit parameter"))?,
                );
            }
            _ => {}
        }
        Ok(())
    })?;
    Ok(params)
}
