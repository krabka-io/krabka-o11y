use super::{
    Arc, BlockBuilderConfig, Cli, ClientSecurity, ObjectStore, ServiceMetrics,
    client_resource_policy,
};

/// Reads the block builder's configuration off the command line.
///
/// `store` and `index_key` are parameters rather than read from `cli` because
/// `--target all` builds its object store once for the whole process and hands
/// the same handle to every role. A block builder that parsed
/// `--object-store-url` a second time would get a second store, and with an
/// in-memory URL that is a store nothing else can read: the blocks would be
/// written where no querier ever looks. `wal_security` is a parameter for the
/// same reason: the process loads it once, from the flags.
pub(crate) fn block_builder_config(
    cli: &Cli,
    store: Arc<dyn ObjectStore>,
    index_key: String,
    metrics: ServiceMetrics,
    wal_security: Option<ClientSecurity>,
) -> BlockBuilderConfig {
    let (client_dispatch_queue_capacity, client_frame_max) = client_resource_policy(cli);
    let mut config = BlockBuilderConfig::new(cli.bootstrap.clone(), store).with_metrics(metrics);
    config.client_dispatch_queue_capacity = client_dispatch_queue_capacity;
    config.client_frame_max = client_frame_max;
    config.wal_topic.clone_from(&cli.wal_topic);
    config.group_id.clone_from(&cli.block_builder_group_id);
    config.index_key = index_key;
    config.wal_fetch_max = cli.wal_fetch_max;
    config.wal_fetch_partition_max = cli.wal_fetch_partition_max;
    config.flush_records = cli.block_builder_flush_records;
    config.flush_max_age = cli.block_builder_flush_max_age;
    config.poll_timeout = cli.wal_poll_timeout;
    config.index_snapshot_max = cli.index_snapshot_max;
    config.index_snapshot_retain = cli.index_snapshot_retain;
    config.security = wal_security;
    config
}
