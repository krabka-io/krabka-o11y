use super::{
    Arc, CancellationToken, Cli, ColdProfileStore, ObjectStore, ServiceMetrics, UnionProfileStore,
    WalTailProfileStore, client_resource_policy, spawn_profile_index_refresh, spawn_wal_tail,
};

/// What a profiles read role queries: the WAL tail in front of the blocks.
pub(crate) type ProfileUnionStore = UnionProfileStore<WalTailProfileStore, ColdProfileStore>;

/// The store a read role answers from, and the background work that keeps it
/// current.
///
/// The querier and the query-frontend are the same read path with different
/// execution strategies, so under `--target all` they share one of these.
/// Sharing it is not only thrift: two read paths would mean two WAL tail
/// consumers in one consumer group, which splits the topic's partitions
/// between them, and each half would then answer from a hot window missing
/// whatever the other half was given.
pub(crate) struct ProfileReadPath {
    pub(crate) union: Arc<ProfileUnionStore>,
    cold: Arc<ColdProfileStore>,
    hot: WalTailProfileStore,
    index_key: String,
    store: Arc<dyn ObjectStore>,
}

impl ProfileReadPath {
    pub(crate) fn new(
        union: Arc<ProfileUnionStore>,
        cold: Arc<ColdProfileStore>,
        hot: WalTailProfileStore,
        index_key: String,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            union,
            cold,
            hot,
            index_key,
            store,
        }
    }

    /// Starts the two loops the read path cannot answer correctly without, and
    /// returns their handles for the caller to supervise by name.
    ///
    /// Neither loop is optional and neither reports its own death. A stopped
    /// index refresher leaves the role answering from the snapshot it booted
    /// with, so every block written since is silently absent; a stopped WAL
    /// tail leaves the last few minutes of profiles absent in the same way.
    /// Both failures look exactly like "no profiles matched", which is why the
    /// handles come back rather than being dropped here.
    pub(crate) fn spawn_background(
        &self,
        cli: &Cli,
        metrics: &ServiceMetrics,
        shutdown: &CancellationToken,
    ) -> [(&'static str, tokio::task::JoinHandle<()>); 2] {
        let (client_dispatch_queue_capacity, client_frame_max) = client_resource_policy(cli);
        [
            (
                "profiles index refresher",
                spawn_profile_index_refresh(
                    Arc::clone(&self.cold),
                    Arc::clone(&self.store),
                    self.index_key.clone(),
                    cli.index_snapshot_max,
                    cli.index_refresh_interval,
                    shutdown.clone(),
                ),
            ),
            (
                "profiles WAL tail",
                spawn_wal_tail(
                    cli,
                    self.hot.clone(),
                    client_dispatch_queue_capacity,
                    client_frame_max,
                    metrics.wal_consumer.clone(),
                    shutdown.clone(),
                ),
            ),
        ]
    }
}
