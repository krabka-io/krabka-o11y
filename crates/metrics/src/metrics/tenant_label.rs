use super::EncodeLabelSet;

/// Per-tenant label for the accepted-series counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TenantLabel {
    pub tenant: String,
}
