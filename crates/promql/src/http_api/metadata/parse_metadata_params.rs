use super::{ApiError, MetadataParams, parse_limit_parameter};
use crate::http_api::parse_query_pairs;

pub(crate) fn parse_metadata_params(raw_query: Option<&str>) -> Result<MetadataParams, ApiError> {
    let mut params = MetadataParams::default();
    parse_query_pairs(raw_query, |name, value| {
        match name.as_str() {
            "metric" => params.metric = Some(value),
            "limit" => params.limit = Some(parse_limit_parameter(&value)?),
            "limit_per_metric" => params.limit_per_metric = Some(parse_limit_parameter(&value)?),
            _ => {}
        }
        Ok(())
    })?;
    Ok(params)
}
