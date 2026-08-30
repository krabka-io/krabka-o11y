use super::{
    BTreeMap, COMPACTION_FRONTIER_MANIFEST_VERSION, CompactionFrontier, Deserialize, Offset,
    PartitionIndex, Serialize,
};

#[derive(Deserialize, Serialize)]
pub(crate) struct CompactionFrontierManifest {
    pub(crate) version: u32,
    pub(crate) compacted_through_ns: i64,
    pub(crate) partition_offsets: BTreeMap<PartitionIndex, Offset>,
}

impl From<&CompactionFrontier> for CompactionFrontierManifest {
    fn from(frontier: &CompactionFrontier) -> Self {
        Self {
            version: COMPACTION_FRONTIER_MANIFEST_VERSION,
            compacted_through_ns: frontier.compacted_through_ns,
            partition_offsets: frontier.partition_offsets.clone(),
        }
    }
}
