use super::{Time, hours};

/// How long an object the index does not name is left alone before
/// [`reconcile_orphans`](super::reconcile_orphans) may delete it.
///
/// A writer puts a block and only then publishes the index entry that names
/// it, so between those two steps the block is referenced by nothing and looks
/// exactly like an orphan. Deleting it there would delete a block that is
/// about to become live. Nothing in the protocol tells the sweeper which is
/// which, so the sweeper waits. An hour is far longer than a flush and its
/// index save can take, and it costs only that a real orphan survives an hour
/// longer than it had to.
pub const DEFAULT_BLOCK_SWEEP_GRACE: Time = hours(1);
