use super::BlockMeta;

/// Signal-specific index seam.
///
/// The trait says nothing about persistence. Each signal's index publishes
/// itself as a manifest over content-addressed shard payloads, and the shape
/// of a shard is the signal's own: a `serde` bound here would have said every
/// index is one serialisable document, which is exactly what it no longer is.
pub trait BlockIndex: Default {
    fn add_block(&mut self, meta: &BlockMeta);
    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String>;
    fn block_count(&self, tenant: &str) -> usize;
}
