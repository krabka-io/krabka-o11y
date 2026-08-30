use super::*;

#[must_use]
pub fn log_block_object_path(prefix: &ObjectPath, key: &BlockKey) -> ObjectPath {
    key.object_key()
        .split('/')
        .fold(prefix.clone(), ObjectPath::join)
}
