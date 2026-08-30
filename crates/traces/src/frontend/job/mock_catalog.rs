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
