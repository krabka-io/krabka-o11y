use super::{BlockStoreError, Path, Result};

/// Reads back the generation that
/// [`super::snapshot_key_for_generation`] encoded.
pub(crate) fn snapshot_generation_from_path(path: &Path) -> Result<u64> {
    path.filename()
        .and_then(|filename| filename.strip_suffix(".json"))
        .and_then(|generation| generation.parse().ok())
        .ok_or_else(|| {
            BlockStoreError::InvalidBlock(format!(
                "index snapshot `{path}` is not named `<generation>.json`"
            ))
        })
}
