use super::*;

/// `tenant` label set for the per-tenant ingested-samples counter family.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TenantLabel {
    pub tenant: String,
}
