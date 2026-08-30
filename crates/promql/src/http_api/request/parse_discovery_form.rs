use super::{ApiError, DiscoveryParams, parse_discovery_params};

pub(crate) fn parse_discovery_form(body: &[u8]) -> Result<DiscoveryParams, ApiError> {
    parse_discovery_params(std::str::from_utf8(body).ok())
}
