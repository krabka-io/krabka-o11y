use super::*;

#[derive(Clone)]
pub(crate) struct ColdObjectStoreState {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: ObjectPath,
}
