use super::*;

/// Signal-specific index seam.
pub trait BlockIndex: Default + Serialize + DeserializeOwned {
    fn add_block(&mut self, meta: &BlockMeta);
    fn candidate_blocks(&self, tenant: &str, min_ts: i64, max_ts: i64) -> Vec<String>;
    fn block_count(&self, tenant: &str) -> usize;
}
