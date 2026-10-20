use super::*;

#[derive(bon::Builder)]
pub(crate) struct MemoryBlockStoreFixture {
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) base: url::Url,
}

impl MemoryBlockStoreFixture {
    pub(crate) fn build() -> Self {
        Self::builder()
            .object_store(Arc::new(InMemory::new()))
            .base(url::Url::parse("memory:///").unwrap())
            .build()
    }

    pub(crate) fn writer(&self) -> BlockStore {
        BlockStore::new(self.object_store.clone(), self.base.clone())
    }

    pub(crate) fn reader(&self) -> BlockStore {
        BlockStore::new(Arc::clone(&self.object_store), self.base.clone())
    }
}
