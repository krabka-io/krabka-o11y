use super::{FsPath, PathBuf};

pub(crate) fn loki_ruler_rules_path(root: &FsPath) -> PathBuf {
    root.join("loki-ruler-rules.json")
}
