use super::{Deserialize, GroupOffset, Serialize, WalOffset};

/// The broker state that a deployment cut records and that a restore must
/// reproduce.
///
/// `wal_offsets` holds the next offset of every partition of every topic in
/// the cut. `group_offsets` holds every committed consumer-group offset on
/// those topics. Both lists are sorted and unique.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct BrokerSnapshot {
    pub wal_offsets: Vec<WalOffset>,
    pub group_offsets: Vec<GroupOffset>,
}

impl BrokerSnapshot {
    /// Sorts both lists into the order that a stored cut uses.
    #[must_use]
    pub fn sorted(mut self) -> Self {
        self.wal_offsets.sort();
        self.group_offsets.sort();
        self
    }
}
