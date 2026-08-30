use super::*;

#[must_use]
pub fn log_index_manifest_object_path(prefix: &ObjectPath) -> ObjectPath {
    LOG_INDEX_MANIFEST_RELATIVE_PATH
        .split('/')
        .fold(prefix.clone(), ObjectPath::join)
}
