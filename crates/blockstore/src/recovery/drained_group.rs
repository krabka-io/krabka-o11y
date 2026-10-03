use super::{Deserialize, Serialize};

/// A consumer group that must have committed every record of its topic at the
/// cut.
///
/// A block builder writes a block before it commits the offsets behind it. A
/// block builder that stopped between the two leaves blocks that a restored
/// deployment writes again under other keys. A drained group has no such
/// window, so a backup refuses a cut where a drained group has a record after
/// its committed offset.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct DrainedGroup {
    pub group: String,
    pub topic: String,
}
