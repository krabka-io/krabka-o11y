use super::*;

#[must_use]
pub fn log_index_manifest_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join(LOG_INDEX_MANIFEST_RELATIVE_PATH)
}
