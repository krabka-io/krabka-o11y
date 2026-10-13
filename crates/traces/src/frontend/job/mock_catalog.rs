use krabka_blockstore::TimeRange;

use super::{BlockCatalog, BlockMetaInfo, CatalogError, async_trait};

/// A canned block catalog for tests.
pub struct MockCatalog {
    pub(crate) blocks: Vec<BlockMetaInfo>,
}

impl MockCatalog {
    #[must_use]
    pub fn new(blocks: Vec<BlockMetaInfo>) -> Self {
        Self { blocks }
    }

    #[must_use]
    pub fn one_block_with_row_groups(
        block_id: &str,
        time_range: TimeRange,
        row_group_bytes: &[u64],
    ) -> Self {
        Self::new(vec![BlockMetaInfo::with_row_groups(
            block_id,
            time_range,
            row_group_bytes,
        )])
    }
}

#[async_trait]
impl BlockCatalog for MockCatalog {
    async fn blocks(
        &self,
        _tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        Ok(self
            .blocks
            .iter()
            .filter(|b| b.end_ns >= start_ns && b.start_ns <= end_ns)
            .cloned()
            .collect())
    }
}
