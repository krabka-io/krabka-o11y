use super::*;

/// Batch of series for one tenant.
#[derive(Clone, Debug, PartialEq)]
pub struct SeriesPayload {
    pub tenant: String,
    pub series: Vec<Series>,
}
