use super::*;

pub(crate) struct ConfiguredObjectStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: ObjectPath,
}
