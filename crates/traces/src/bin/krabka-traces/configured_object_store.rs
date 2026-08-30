use super::{Arc, ObjectStore, Url, Path, blockbuilder};

pub(crate) struct ConfiguredObjectStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) root: Url,
    pub(crate) prefix: Path,
}

impl ConfiguredObjectStore {
    pub(crate) fn object_key(&self, key: &str) -> String {
        blockbuilder::prefixed_object_key(self.prefix.as_ref(), key)
    }
}
