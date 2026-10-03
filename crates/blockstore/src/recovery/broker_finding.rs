use super::{Deserialize, Serialize};

/// One difference between the broker state that a cut recorded and the
/// broker state found.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrokerFinding {
    /// A partition next offset differs. `None` means that the side does not
    /// have the partition.
    WalOffset {
        topic: String,
        partition: i32,
        expected: Option<i64>,
        actual: Option<i64>,
    },
    /// A committed group offset differs. `None` means that the side has no
    /// committed offset.
    GroupOffset {
        group: String,
        topic: String,
        partition: i32,
        expected: Option<i64>,
        actual: Option<i64>,
    },
    /// A drained group has a record after its committed offset.
    UndrainedGroup {
        group: String,
        topic: String,
        partition: i32,
        committed: Option<i64>,
        wal_next_offset: i64,
    },
}
