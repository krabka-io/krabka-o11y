use super::{async_trait, ByteSizeExt, BlockCatalog, BTreeMap, BlockMetaInfo, BlockStore, TraceIndex, BlockStoreResult, blocks_for_tenant, CatalogError};

/// The production block catalog.
///
/// A pre-resolved per-tenant [`krabka_blockstore::TraceIndex`] backs it. It is
/// built once at startup from the index. It ports
/// `backend_blocks_from_trace_index` from the legacy query-frontend.
pub struct TraceIndexCatalog {
    pub(crate) by_tenant: BTreeMap<String, Vec<BlockMetaInfo>>,
}

impl TraceIndexCatalog {
    #[must_use]
    pub fn new(by_tenant: BTreeMap<String, Vec<BlockMetaInfo>>) -> Self {
        Self { by_tenant }
    }

    /// Build the catalog from a `BlockStore` and a `TraceIndex`.
    ///
    /// This reads each block's parquet row-group metadata, which is the
    /// per-tenant block list.
    ///
    /// # Errors
    /// Propagates object-store and parquet read errors.
    pub async fn from_trace_index(
        blocks: &BlockStore,
        index: &TraceIndex,
    ) -> BlockStoreResult<Self> {
        let mut by_tenant = BTreeMap::new();
        for tenant in index.tenants() {
            let metas = blocks_for_tenant(blocks, index, &tenant).await?;
            by_tenant.insert(tenant, metas);
        }
        Ok(Self::new(by_tenant))
    }
}

#[async_trait]
impl BlockCatalog for TraceIndexCatalog {
    async fn blocks(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<BlockMetaInfo>, CatalogError> {
        Ok(self
            .by_tenant
            .get(tenant)
            .map(|blocks| {
                blocks
                    .iter()
                    .filter(|b| b.end_ns >= start_ns && b.start_ns <= end_ns)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
}
