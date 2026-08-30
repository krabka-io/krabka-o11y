use super::EncodeLabelSet;

/// `route` + `status` label set for the query-request counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteStatusLabel {
    pub route: String,
    pub status: String,
}
