use krabka_observability::ReadinessGate;

use super::{
    Arc, CancellationToken, Cli, ColdProfileStore, DebuginfodConfig, ObjectStore, ProfileIndex,
    ProfileReadPath, RetentionConfig, UnionProfileStore, WalTailProfileStore,
};

/// Loads the block index, opens the cold store over it, and puts a WAL tail in
/// front of both.
///
/// `None` is a stop, not a failure. The snapshot load is raced against
/// `shutdown` because an object store that has gone away retries for minutes:
/// a role parked in there would hear `SIGTERM` and do nothing with it, and the
/// orchestrator's only remaining move is to kill the process.
///
/// `index_gate` is marked ready the moment the snapshot is in hand, and not
/// before. A read role that answered while its index was still loading would
/// return an empty result rather than an error, which nothing downstream --
/// not a query-frontend, not a Grafana panel -- can tell apart from "no
/// profiles matched".
///
/// # Errors
/// Returns an error when the index snapshot cannot be read or decoded, or when
/// the debuginfod-backed symbol resolver cannot be built.
pub(crate) async fn build_profile_read_path(
    cli: &Cli,
    store: Arc<dyn ObjectStore>,
    index_key: String,
    debuginfod: DebuginfodConfig,
    index_gate: ReadinessGate,
    shutdown: &CancellationToken,
) -> Result<Option<ProfileReadPath>, Box<dyn std::error::Error>> {
    let index = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Ok(None),
        loaded = ProfileIndex::load_latest_snapshot_or_empty_with_max_bytes(
            &store,
            &index_key,
            cli.index_snapshot_max,
        ) => loaded?,
    };
    index_gate.mark_ready();
    let cold = Arc::new(ColdProfileStore::new_with_debuginfod_config(
        Arc::clone(&store),
        Arc::new(index),
        cli.debuginfod_urls.clone(),
        debuginfod,
    )?);
    let hot = WalTailProfileStore::with_retention(RetentionConfig {
        max_age: cli.hot_store_max_age,
        max_records: cli.hot_store_max_records,
    });
    let union = Arc::new(UnionProfileStore::new(
        Arc::new(hot.clone()),
        Arc::clone(&cold),
    ));
    Ok(Some(ProfileReadPath::new(
        union, cold, hot, index_key, store,
    )))
}
