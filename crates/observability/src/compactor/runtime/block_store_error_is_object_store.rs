use super::*;

pub(crate) fn block_store_error_is_object_store(error: &BlockStoreError) -> bool {
    matches!(error, BlockStoreError::ObjectStore(_))
}
