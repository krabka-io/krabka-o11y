use super::{BlockMetadataCache, ObjectMeta};

/// One block's cache participation: the object as `head` just reported it, and
/// the cache to validate against and fill.
#[derive(Clone, Debug)]
pub(crate) struct CachedBlock {
    pub(crate) meta: ObjectMeta,
    pub(crate) cache: BlockMetadataCache,
}
