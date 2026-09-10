use super::{BlockReadFailure, BlockStoreError};

/// Attributes a backend error to the block it was read from.
///
/// Every function here reads one named block, so a failure below it is that
/// block's failure, and the key belongs in the error rather than in a message
/// the caller would have to parse back out.
///
/// This takes the error rather than returning a closure over the key on
/// purpose. A returned `impl Fn(..)` is an opaque type carrying the key's
/// lifetime, and holding one across an `.await` leaves the whole future
/// `Send` only for one specific lifetime — which breaks every `async fn` up
/// the call chain.
pub(crate) fn unreadable<E>(object_key: &str, error: E) -> BlockStoreError
where
    E: Into<BlockReadFailure>,
{
    BlockStoreError::block_unreadable(object_key, error.into())
}
