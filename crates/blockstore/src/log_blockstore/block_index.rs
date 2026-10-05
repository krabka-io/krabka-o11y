use super::*;

/// No `Eq`: [`BlockDescriptor`] holds a [`ByteSize`], which is only `PartialEq`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BlockIndex {
    pub(crate) blocks: Vec<BlockDescriptor>,
}

impl BlockIndex {
    pub fn insert(&mut self, block: BlockDescriptor) {
        self.blocks
            .retain(|existing| existing.key.object_key() != block.key.object_key());
        self.blocks.push(block);
        self.blocks
            .sort_by_cached_key(|block| block.key.object_key());
    }

    #[must_use]
    pub fn blocks(&self) -> &[BlockDescriptor] {
        &self.blocks
    }

    #[must_use]
    pub fn match_blocks(
        &self,
        tenant: &str,
        time_range: TimeRange,
        fingerprints: &[SeriesFingerprint],
    ) -> Vec<BlockDescriptor> {
        self.blocks
            .iter()
            .filter(|block| {
                block.key.tenant == tenant
                    && block.key.time_range.overlaps(time_range)
                    && (fingerprints.is_empty()
                        || fingerprints
                            .iter()
                            .any(|fingerprint| block.fingerprints.contains(fingerprint)))
            })
            .cloned()
            .collect()
    }
}
