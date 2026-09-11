use super::{BlockKey, Component, Path, PathBuf};

/// Local path of the block that `key` names, under `root`.
///
/// The result is always under `root`. The path grows one component at a time
/// and keeps only the normal components, so a root, a prefix, `.` and `..` are
/// dropped and never followed. [`BlockKey::object_key`] escapes the tenant
/// through [`crate::escape_object_path_segment`], so none of those reach here
/// in the first place. The filter is a second guard, and it does not depend on
/// the escape.
#[must_use]
pub fn block_path(root: impl AsRef<Path>, key: &BlockKey) -> PathBuf {
    let mut path = root.as_ref().to_path_buf();
    for segment in key.object_key().split('/') {
        for component in Path::new(segment).components() {
            if let Component::Normal(name) = component {
                path.push(name);
            }
        }
    }
    path
}
