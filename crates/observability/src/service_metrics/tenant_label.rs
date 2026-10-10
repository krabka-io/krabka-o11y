use super::EncodeLabelSet;

/// Per-tenant ingest label, such as `tenant="anonymous"`. It pairs with a
/// signal's per-tenant accepted-volume counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TenantLabel {
    pub tenant: String,
}
