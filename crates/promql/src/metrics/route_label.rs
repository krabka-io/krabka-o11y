use super::EncodeLabelSet;

/// Per-query-route label (latency histogram family).
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteLabel {
    pub route: String,
}
