use super::{DiscoveryParams, LabelMatcher, ApiError, selector_matchers};

pub(crate) fn discovery_matchers(
    params: &DiscoveryParams,
) -> Result<Vec<Vec<LabelMatcher>>, ApiError> {
    if params.matches.is_empty() {
        return Ok(vec![Vec::new()]);
    }
    let mut out = Vec::new();
    for selector in &params.matches {
        out.extend(selector_matchers(selector).map_err(ApiError::from)?);
    }
    Ok(out)
}
