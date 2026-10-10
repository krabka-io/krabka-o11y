use super::{
    Arc, ByteSize, IndexShardRange, ObjectStore, Result, SnapshotManifest, read_shard_payload,
    shard_payload_object_key,
};

/// The one tenant, and the span of it, a windowed load keeps.
#[derive(Clone, Copy)]
pub(crate) struct ShardWindow<'a> {
    pub(crate) tenant: &'a str,
    pub(crate) span: IndexShardRange,
}

/// A load of the shard payloads one manifest names.
pub(crate) struct ManifestRead<'a> {
    pub(crate) store: &'a Arc<dyn ObjectStore>,
    pub(crate) key: &'a str,
    pub(crate) manifest: &'a SnapshotManifest,
    /// When given, only this tenant's shards that meet the span are read;
    /// everything else is listed in the manifest and then not read.
    pub(crate) window: Option<ShardWindow<'a>>,
    pub(crate) max_bytes: ByteSize,
}

/// How many shards a load listed and how many it read.
pub(crate) struct WindowShardReads {
    pub(crate) listed: usize,
    pub(crate) read: usize,
}

impl ManifestRead<'_> {
    /// Reads the payloads in the window and hands each to `fold` with its
    /// tenant and object key.
    pub(crate) async fn read_shards(
        &self,
        mut fold: impl FnMut(&str, &str, &[u8]) -> Result<()>,
    ) -> Result<WindowShardReads> {
        let mut reads = WindowShardReads { listed: 0, read: 0 };
        for tenant in self.manifest.tenants() {
            if self.window.is_some_and(|window| window.tenant != tenant) {
                continue;
            }
            for shard in self.manifest.shards_of(tenant) {
                reads.listed += 1;
                let range = shard.range();
                if self
                    .window
                    .is_some_and(|window| !range.overlaps(window.span.start, window.span.end))
                {
                    continue;
                }
                let object_key = shard_payload_object_key(self.key, tenant, range, &shard.content);
                let bytes = read_shard_payload(self.store, &object_key, self.max_bytes).await?;
                reads.read += 1;
                fold(tenant, &object_key, &bytes)?;
            }
        }
        Ok(reads)
    }
}
