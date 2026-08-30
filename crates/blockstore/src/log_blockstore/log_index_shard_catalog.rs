use super::*;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LogIndexShardCatalog {
    pub(crate) format_version: u32,
    pub(crate) shards: Vec<TimeRange>,
}

impl LogIndexShardCatalog {
    pub(crate) fn new(shard_ranges: &[TimeRange]) -> Self {
        let mut shards = shard_ranges.to_vec();
        shards.sort_by_key(|range| (range.start_ns, range.end_ns));
        shards.dedup();

        Self {
            format_version: LOG_INDEX_MANIFEST_VERSION,
            shards,
        }
    }

    pub(crate) fn into_shards(self) -> Result<Vec<TimeRange>, BlockStoreError> {
        if self.format_version != LOG_INDEX_MANIFEST_VERSION {
            return Err(BlockStoreError::InvalidManifestVersion {
                actual: self.format_version,
                expected: LOG_INDEX_MANIFEST_VERSION,
            });
        }
        Ok(self.shards)
    }
}
