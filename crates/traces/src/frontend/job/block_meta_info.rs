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
    /// A block spanning `start_ns..end_ns` whose row-groups, in order, have
    /// the compressed sizes `row_group_bytes`. Its size is their sum.
    #[must_use]
    pub fn with_row_groups(
        block_id: &str,
        start_ns: i64,
        end_ns: i64,
        row_group_bytes: &[u64],
    ) -> Self {
        let row_groups = row_group_bytes
            .iter()
            .enumerate()
            .map(|(index, &bytes)| RowGroupInfo {
                index: u32::try_from(index).unwrap_or(u32::MAX),
                compressed: ByteSize::from_bytes(bytes),
            })
            .collect();
        Self {
            block_id: block_id.to_string(),
            start_ns,
            end_ns,
            size: ByteSize::from_bytes(row_group_bytes.iter().sum()),
            row_groups,
        }
    }

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
