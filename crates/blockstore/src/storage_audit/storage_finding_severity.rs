use super::{Deserialize, Serialize};

/// How much a finding says about the health of the store.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageFindingSeverity {
    /// The store is damaged, or holds an object that no reader will use.
    Damage,
    /// The state is legal but needs a person to look at it. A write in
    /// flight, a replayed WAL range and a lagging frontier all look like this.
    Warning,
}
