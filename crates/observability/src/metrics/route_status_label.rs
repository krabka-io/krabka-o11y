use super::EncodeLabelSet;

/// Query route + outcome label (`route="query", status="ok"|"error"`).
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteStatusLabel {
    pub route: String,
    pub status: String,
}
