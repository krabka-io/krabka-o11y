use super::*;

#[must_use]
pub fn block_path(root: impl AsRef<Path>, key: &BlockKey) -> PathBuf {
    root.as_ref().join(key.object_key())
}
