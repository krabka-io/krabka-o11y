use super::{
    BlockKey, BlockStoreError, ErrorKind, LogRow, QuerierState, read_log_block,
    read_log_block_from_object_store,
};

/// Reads one planned block, or reports that the block object is gone.
///
/// Returns `Ok(None)` when the object is absent, and `Ok(Some(rows))` when it
/// reads. The retention sweep deletes block objects while queries run, so a
/// block the plan named can disappear between the plan and the read. That is
/// the one failure a read path may skip, because it belongs to that object
/// alone. The rest of the answer is still correct, and a caller that skips the
/// block must say so, either in a `warnings` array or in the log line this
/// function writes.
///
/// Every other failure stays an error. A malformed block, a refused
/// credential, and a store outage all say nothing about this one object, so
/// they fail the query instead of shortening its result without a word. This
/// is the same split that [`BlockReadFailure::skip_reason`] makes for the
/// trace path.
///
/// The choice of store is the caller's configuration, not the caller's
/// concern: a querier with a cold object store reads the block from it, and a
/// querier without one reads the block from its local root.
///
/// [`BlockReadFailure::skip_reason`]: krabka_blockstore::BlockReadFailure::skip_reason
///
/// # Errors
/// Returns the block-store error for every failure except an absent object.
pub(crate) async fn read_planned_log_block(
    state: &QuerierState,
    key: &BlockKey,
) -> Result<Option<Vec<LogRow>>, BlockStoreError> {
    let result = if let Some(cold_store) = &state.cold_store {
        read_log_block_from_object_store(cold_store.store.as_ref(), &cold_store.prefix, key).await
    } else {
        read_log_block(&state.root, key)
    };

    match result {
        Ok(rows) => Ok(Some(rows)),
        Err(error) if block_object_is_absent(&error) => {
            tracing::warn!(
                object_key = %key.object_key(),
                %error,
                "log block object is absent; the query skips it and answers without its rows"
            );
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

/// Whether `error` says the block object is not there.
///
/// The two arms are the two stores. An object store reports a deleted object
/// as [`object_store::Error::NotFound`], and the local filesystem reports a
/// deleted file as an I/O error of kind [`ErrorKind::NotFound`]. No other
/// variant means the object is absent: a decode failure proves the object was
/// present, and a refused or failed request says nothing about it either way.
fn block_object_is_absent(error: &BlockStoreError) -> bool {
    match error {
        BlockStoreError::ObjectStore(object_store::Error::NotFound { .. }) => true,
        BlockStoreError::Io(source) => source.kind() == ErrorKind::NotFound,
        _ => false,
    }
}
