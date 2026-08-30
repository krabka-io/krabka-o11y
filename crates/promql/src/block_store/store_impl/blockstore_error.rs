use super::PromqlError;

pub(crate) fn blockstore_error(error: krabka_blockstore::BlockStoreError) -> PromqlError {
    let message = error.to_string();
    drop(error);
    PromqlError::Store(message)
}
