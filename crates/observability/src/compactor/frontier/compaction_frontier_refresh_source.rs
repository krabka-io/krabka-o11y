use super::{Arc, ObjectPath, ObjectStore};

pub(crate) struct CompactionFrontierRefreshSource {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: ObjectPath,
}
