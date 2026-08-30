use super::*;

/// Query route label (`route="query"`). It pairs with the per-route latency
/// histogram family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RouteLabel {
    pub route: String,
}
