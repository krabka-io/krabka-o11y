use super::{
    Arc, ObjectStore, StorageAuditError, StorageAuditOptions, StorageAuditReport, audit_inventory,
    instrument,
};

/// Audits a store and reports what is wrong with it. Reads only.
///
/// The audit lists the store once and matches every key against the key
/// grammars of the four signals. It then checks, for each signal in scope:
///
/// - that every block has a Parquet footer this build reads, and with
///   `verify_data`, rows that decode;
/// - that the index names every block, and that every block it names exists;
/// - that every metrics `.index` manifest decodes and names its own key,
///   block and tenant;
/// - that every live profiles block has its `.symdb` symbol table, and with
///   `verify_data`, that the symbol table decodes;
/// - that the index of each tenant names only blocks of that tenant;
/// - that the trace and profile snapshot payloads match their checksums;
/// - that the logs manifests and shard catalogs decode and agree with the
///   store;
/// - that metrics delete markers and erasure requests decode;
/// - that no two blocks of one WAL partition cover overlapping offsets, and
///   that the logs compaction frontier is not ahead of every block.
///
/// `store` is the root that the services write to, such as a
/// [`PrefixStore`](object_store::prefix::PrefixStore) over the bucket prefix.
///
/// # Errors
/// Returns [`StorageAuditError`] when the listing fails or a read fails for
/// a reason other than a damaged object. The audit then knows too little to
/// report, and a partial report would read as a clean one.
#[instrument(level = "info", skip_all, err)]
pub async fn audit_store(
    store: &Arc<dyn ObjectStore>,
    options: &StorageAuditOptions,
) -> Result<StorageAuditReport, StorageAuditError> {
    audit_inventory(store, options)
        .await
        .map(|(report, _)| report)
}
