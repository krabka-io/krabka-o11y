use super::*;

/// Per-tenant ingest label, such as `tenant="anonymous"`. It pairs with the
/// spans-accepted counter family, so accepted-span volume is attributable per
/// tenant.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TenantLabel {
    pub tenant: String,
}
