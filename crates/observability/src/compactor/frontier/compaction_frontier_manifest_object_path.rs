use super::*;

pub(crate) fn compaction_frontier_manifest_object_path(prefix: &ObjectPath) -> ObjectPath {
    COMPACTION_FRONTIER_MANIFEST_RELATIVE_PATH
        .split('/')
        .fold(prefix.clone(), ObjectPath::join)
}
