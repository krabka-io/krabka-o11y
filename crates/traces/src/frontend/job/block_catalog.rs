use super::{BlockMetaInfo, CatalogError, async_trait};

/// The block-catalog door: which blocks overlap `[start_ns, end_ns]` for a
/// tenant.
///
/// Tests use [`crate::frontend::MockCatalog`]. Production uses
/// [`crate::frontend::TraceIndexCatalog`].
#[async_trait]
pub trait BlockCatalog: Send + Sync {
    async fn blocks(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError>;
}
