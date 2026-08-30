use super::{FsPath, PathBuf};

pub(crate) fn log_delete_requests_path(root: &FsPath) -> PathBuf {
    root.join("log-delete-requests.json")
}
