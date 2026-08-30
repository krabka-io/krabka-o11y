use super::*;

#[derive(Clone, Default)]
pub struct SharedLogDeleteRequests {
    pub(crate) inner: Arc<Mutex<CompactorDeleteRequests>>,
    pub(crate) storage_path: Option<Arc<PathBuf>>,
}
