use super::EncodeLabelSet;

/// Per-tenant ingest label (`tenant="…"`). It pairs with the accepted-lines
/// counter, so the service can attribute ingest volume per tenant.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TenantLabel {
    pub tenant: String,
}
