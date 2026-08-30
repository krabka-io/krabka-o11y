use super::{Arc, ListingTable, ObjectPath, ObjectStore};

#[derive(Debug)]
pub(crate) enum LogBlockTableSource {
    Local(Box<ListingTable>),
    ObjectStore {
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
    },
}
