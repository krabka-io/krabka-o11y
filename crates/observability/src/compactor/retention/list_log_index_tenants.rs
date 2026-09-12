use super::{
    BTreeSet, BlockStoreError, CompactorRunError, ObjectPath, ObjectStore,
    unescape_object_path_segment,
};

/// Every tenant that has written anything under `prefix`.
///
/// There is no API that lists the tenants of a deployment, and the overrides
/// file names only the tenants an operator configured. A tenant on the default
/// window has no entry there and still has blocks, so the sweep reads the
/// tenants out of the object store instead: one delimited listing of `prefix`
/// gives a `tenant=<escaped>` prefix for each one.
///
/// A prefix whose escape does not decode is skipped. Nothing Krabka writes
/// produces one, so such a prefix was written by something else and the sweep
/// has no index for it to rewrite.
///
/// # Errors
/// Returns the object store's error when `prefix` cannot be listed.
pub(crate) async fn list_log_index_tenants(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
) -> Result<BTreeSet<String>, CompactorRunError> {
    let listing = store
        .list_with_delimiter(Some(prefix))
        .await
        .map_err(|error| CompactorRunError::BlockStore(BlockStoreError::ObjectStore(error)))?;
    Ok(listing
        .common_prefixes
        .iter()
        .filter_map(|common_prefix| common_prefix.filename())
        .filter_map(|segment| segment.strip_prefix("tenant="))
        .filter_map(unescape_object_path_segment)
        .collect())
}
