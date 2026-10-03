use super::{BTreeMap, BTreeSet};

/// The blocks that a signal's index makes live, and the tenants whose index
/// the audit could not read.
///
/// An audit never calls a block an orphan when it does not know the index of
/// the block's tenant. A repair deletes orphans, so a guess here would delete
/// live data.
#[derive(Clone, Debug, Default)]
pub struct LiveBlockSet {
    /// Live block keys, each with its tenant.
    pub live: BTreeMap<String, String>,
    /// Tenants whose index did not read.
    pub unknown_tenants: BTreeSet<String>,
    /// Whether the whole index did not read.
    pub all_unknown: bool,
}

impl LiveBlockSet {
    /// A set that knows nothing, for a signal whose index did not read.
    pub fn unknown() -> Self {
        Self {
            all_unknown: true,
            ..Self::default()
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        self.live.contains_key(key)
    }

    /// Whether the index of `tenant` read, so absence from it means
    /// something.
    pub fn knows(&self, tenant: Option<&str>) -> bool {
        !self.all_unknown && tenant.is_none_or(|tenant| !self.unknown_tenants.contains(tenant))
    }
}
