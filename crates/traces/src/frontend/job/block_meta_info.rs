use super::{ByteSize, ByteSizeExt, RowGroupInfo};

/// Block metadata the planner needs, from the querier's block catalog.
#[derive(Clone, Debug, PartialEq)]
pub struct BlockMetaInfo {
    pub block_id: String,
    pub start_ns: i64,
    pub end_ns: i64,
    /// The block's compressed size on the object store.
    pub size: ByteSize,
    pub row_groups: Vec<RowGroupInfo>,
}

impl BlockMetaInfo {
    /// Total compressed size across this block's row-groups. It falls back to
    /// [`Self::size`] when the row-group sizes are not available.
    #[must_use]
    pub fn total(&self) -> ByteSize {
        let rg_total: ByteSize = self.row_groups.iter().map(|rg| rg.compressed).sum();
        if rg_total == <ByteSize as ByteSizeExt>::ZERO {
            self.size
        } else {
            rg_total
        }
    }
}
