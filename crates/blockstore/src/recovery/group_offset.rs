use super::{Deserialize, Serialize};

/// The committed offset of one consumer group in one partition.
///
/// `next_offset` is the next offset the group reads, as Kafka's `OffsetFetch`
/// reports it.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct GroupOffset {
    pub group: String,
    pub topic: String,
    pub partition: i32,
    pub next_offset: i64,
}
