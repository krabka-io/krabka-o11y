use super::{ObjectPath, ObjectStore};

pub(crate) struct ConfiguredObjectStore {
    pub(crate) store: std::sync::Arc<dyn ObjectStore>,
    pub(crate) prefix: ObjectPath,
}

impl ConfiguredObjectStore {
    pub(crate) fn object_key(&self, key: &str) -> String {
        let prefix = self.prefix.as_ref().trim_matches('/');
        let key = key.trim_start_matches('/');
        if prefix.is_empty() {
            key.to_string()
        } else {
            format!("{prefix}/{key}")
        }
    }
}
