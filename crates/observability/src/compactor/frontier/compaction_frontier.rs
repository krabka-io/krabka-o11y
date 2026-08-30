use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompactionFrontier {
    pub compacted_through_ns: i64,
    pub(crate) partition_offsets: BTreeMap<PartitionIndex, Offset>,
}

impl CompactionFrontier {
    #[must_use]
    pub fn new(compacted_through_ns: i64) -> Self {
        Self {
            compacted_through_ns,
            partition_offsets: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_partition_offset(mut self, partition: PartitionIndex, offset: Offset) -> Self {
        self.partition_offsets.insert(partition, offset);
        self
    }

    pub fn advance_partition_offset(&mut self, position: WalPosition) {
        self.partition_offsets
            .entry(position.partition)
            .and_modify(|offset| *offset = (*offset).max(position.offset))
            .or_insert(position.offset);
    }

    pub(crate) fn is_compacted(&self, record: &WalLogRecord) -> bool {
        if let Some(position) = record.position
            && self
                .partition_offsets
                .get(&position.partition)
                .is_some_and(|offset| position.offset <= *offset)
        {
            return true;
        }

        record.timestamp_ns <= self.compacted_through_ns
    }
}

impl TryFrom<CompactionFrontierManifest> for CompactionFrontier {
    type Error = CompactionFrontierStoreError;

    fn try_from(manifest: CompactionFrontierManifest) -> Result<Self, Self::Error> {
        if manifest.version != COMPACTION_FRONTIER_MANIFEST_VERSION {
            return Err(CompactionFrontierStoreError::InvalidVersion {
                actual: manifest.version,
                expected: COMPACTION_FRONTIER_MANIFEST_VERSION,
            });
        }

        Ok(Self {
            compacted_through_ns: manifest.compacted_through_ns,
            partition_offsets: manifest.partition_offsets,
        })
    }
}
