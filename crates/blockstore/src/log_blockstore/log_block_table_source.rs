use super::{Arc, ListingTable, ObjectPath, ObjectStore, ObjectStoreUrl};

/// Where a [`LogBlockTableProvider`]'s planned blocks live.
///
/// Both variants scan the same way: a `ListingTable` over the planned block
/// files, built from [`log_listing_options`]. They differ only in what the
/// files are addressed through -- a local path, or an object store the
/// provider has to register on the session before the scan can resolve it.
///
/// [`LogBlockTableProvider`]: super::LogBlockTableProvider
/// [`log_listing_options`]: super::log_listing_options
#[derive(Debug)]
pub(crate) enum LogBlockTableSource {
    Local(Box<ListingTable>),
    ObjectStore {
        store: Arc<dyn ObjectStore>,
        /// The authority this provider registers `store` under, and that the
        /// listing table's paths are addressed through.
        object_store_url: ObjectStoreUrl,
        /// One object per planned block, in planned order. Held so the cap
        /// pre-pass can `head` them without re-deriving them from the keys.
        block_paths: Vec<ObjectPath>,
        listing_table: Box<ListingTable>,
    },
}
