use super::*;

/// Query route and outcome label, such as
/// `route="search", status="ok"|"error"`.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteStatusLabel {
    pub route: String,
    pub status: String,
}
