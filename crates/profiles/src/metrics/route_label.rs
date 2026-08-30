use super::EncodeLabelSet;

/// `route` label set for the per-route query-duration histogram family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteLabel {
    pub route: String,
}
