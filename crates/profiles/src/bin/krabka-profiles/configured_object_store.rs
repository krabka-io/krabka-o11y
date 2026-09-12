use super::ObjectStore;

pub(crate) struct ConfiguredObjectStore {
    pub(crate) store: std::sync::Arc<dyn ObjectStore>,
}
