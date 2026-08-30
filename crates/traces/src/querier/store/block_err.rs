use super::*;

pub(crate) fn block_err(err: &krabka_blockstore::BlockStoreError) -> TraceqlError {
    TraceqlError::Store(err.to_string())
}
