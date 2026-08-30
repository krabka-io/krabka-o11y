use super::{Arc, LokiRuleTenants, Mutex, PathBuf};

#[derive(Clone, Default)]
pub(crate) struct SharedLokiRules {
    pub(crate) tenants: Arc<Mutex<LokiRuleTenants>>,
    pub(crate) storage_path: Option<Arc<PathBuf>>,
}
