/// Rows per sealed chunk in a [`RowChunks`](super::RowChunks).
///
/// This is the ceiling on how many rows a copy-on-write clone of the head
/// copies, per tenant and per row kind. Everything already sealed is shared by
/// pointer. Larger wastes more work on each clone; smaller spends more atomics
/// walking the chunk list on every scan. A thousand rows is a few tens of
/// microseconds to copy and leaves a six-hour head at a few thousand chunks.
pub(crate) const ROW_CHUNK_LEN: usize = 1_024;
