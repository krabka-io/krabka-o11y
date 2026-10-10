use super::{
    Arc, BTreeMap, BTreeSet, ByteSize, IndexShardRange, ObjectStore, PendingRemoval, Result,
    SnapshotManifest, put_shard_payload, read_shard_payload, shard_payload_content_hash,
    shard_payload_object_key,
};

/// One tenant's step of folding a writer's index into a base manifest.
///
/// The trace and profile indexes keep different records per shard, so each
/// supplies how a shard is decoded, edited and re-encoded. This carries what
/// they share: which blocks the writer contributes, which base shards are
/// read and which are carried unread, and which re-encoded payloads are
/// written.
pub(crate) struct TenantShardMerge<'a> {
    pub(crate) store: &'a Arc<dyn ObjectStore>,
    pub(crate) key: &'a str,
    pub(crate) base: &'a SnapshotManifest,
    pub(crate) manifest: &'a mut SnapshotManifest,
    pub(crate) tenant: &'a str,
    pub(crate) removed: Option<&'a BTreeMap<String, PendingRemoval>>,
    pub(crate) contribute_all: bool,
    pub(crate) additions: &'a BTreeSet<String>,
    pub(crate) max_bytes: ByteSize,
    pub(crate) fetched: usize,
    pub(crate) written: usize,
}

impl TenantShardMerge<'_> {
    /// Whether this writer contributes the block under `object_key`.
    pub(crate) fn contributes(&self, object_key: &str) -> bool {
        self.contribute_all || self.additions.contains(object_key)
    }

    /// Fetches every base shard of the tenant that meets a `touched` range
    /// and hands each payload to `fold` with its range and object key.
    ///
    /// Returns the shards no touched range meets, by range, with the content
    /// hash the base names them by: see [`Self::carry`].
    pub(crate) async fn read_touched(
        &mut self,
        touched: &[IndexShardRange],
        mut fold: impl FnMut(IndexShardRange, &str, &[u8]) -> Result<()>,
    ) -> Result<BTreeMap<IndexShardRange, String>> {
        let mut carried = BTreeMap::new();
        for shard in self.base.shards_of(self.tenant) {
            let range = shard.range();
            if touched
                .iter()
                .any(|touched| range.overlaps(touched.start, touched.end))
            {
                let object_key =
                    shard_payload_object_key(self.key, self.tenant, range, &shard.content);
                let bytes = read_shard_payload(self.store, &object_key, self.max_bytes).await?;
                self.fetched += 1;
                fold(range, &object_key, &bytes)?;
            } else {
                carried.insert(range, shard.content.clone());
            }
        }
        Ok(carried)
    }

    /// Names the shards a merge did not read by the keys the base already has.
    pub(crate) fn carry(&mut self, carried: BTreeMap<IndexShardRange, String>) {
        for (range, content) in carried {
            self.manifest.insert(self.tenant, range, content);
        }
    }

    /// Names one re-encoded shard, writing its payload only when the base does
    /// not already name the same bytes for the same range.
    ///
    /// A shard a merge read and put back unchanged encodes to the same bytes
    /// and so to the same key, and the object is already there. Not writing it
    /// is the difference between a flush that rewrites what it touched and one
    /// that rewrites what it read.
    pub(crate) async fn publish(&mut self, range: IndexShardRange, bytes: Vec<u8>) -> Result<()> {
        let content = shard_payload_content_hash(&bytes);
        if !self
            .base
            .shards_of(self.tenant)
            .iter()
            .any(|shard| shard.range() == range && shard.content == content)
        {
            put_shard_payload(self.store, self.key, self.tenant, range, &content, bytes).await?;
            self.written += 1;
        }
        self.manifest.insert(self.tenant, range, content);
        Ok(())
    }
}
