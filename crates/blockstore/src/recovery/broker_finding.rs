use std::fmt;

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

/// Writes the finding on one line, with the `kind` of its JSON form first.
/// `none` stands for an absent offset.
impl fmt::Display for BrokerFinding {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let offset = |value: &Option<i64>| value.map_or_else(|| "none".into(), |v| v.to_string());
        match self {
            Self::WalOffset {
                topic,
                partition,
                expected,
                actual,
            } => write!(
                formatter,
                "wal_offset {topic}:{partition} expected {} actual {}",
                offset(expected),
                offset(actual)
            ),
            Self::GroupOffset {
                group,
                topic,
                partition,
                expected,
                actual,
            } => write!(
                formatter,
                "group_offset {group} {topic}:{partition} expected {} actual {}",
                offset(expected),
                offset(actual)
            ),
            Self::UndrainedGroup {
                group,
                topic,
                partition,
                committed,
                wal_next_offset,
            } => write!(
                formatter,
                "undrained_group {group} {topic}:{partition} committed {} wal_next_offset \
                 {wal_next_offset}",
                offset(committed)
            ),
        }
    }
}
