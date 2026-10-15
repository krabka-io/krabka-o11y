use super::{
    Arc, BlockBuilderConfig, BlockLevel, BlockTimestampUnit, Cli, ClientSecurity, CompactionPolicy,
    DebuginfodConfig, Limits, ObjectStore, OverridesProvider, Path, ReadinessGate, ServiceMetrics,
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
    catch_up: ReadinessGate,
) -> BlockBuilderConfig {
    let client_policy = client_resource_policy(cli);
    let mut config = BlockBuilderConfig::new(cli.bootstrap.clone(), store)
        .with_metrics(metrics)
        .with_catch_up(catch_up);
    config.client_dispatch_queue_capacity = client_policy.client_dispatch_queue_capacity;
    config.client_frame_max = client_policy.client_frame_max;
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

/// Reads the compaction policy the operator configured.
///
/// The unit is not configurable: a profiles block counts its timestamps in
/// epoch milliseconds, so that is the unit the policy converts its window
/// into. The row field those timestamps are written to is named
/// `timestamp_ns` and holds a millisecond value, so the field name is not
/// evidence of the unit here.
pub(crate) fn compaction_policy_from_cli(cli: &Cli) -> CompactionPolicy {
    CompactionPolicy::new(
        cli.compactor_max_blocks_per_job,
        cli.compactor_target_rows,
        BlockLevel(cli.compactor_max_level),
        cli.compactor_level_window,
        BlockTimestampUnit::Millis,
    )
}

pub(crate) fn debuginfod_config(cli: &Cli) -> Result<DebuginfodConfig, String> {
    let defaults = DebuginfodConfig::default();
    DebuginfodConfig::new(
        cli.debuginfod_max_artifact_size
            .unwrap_or(defaults.max_artifact_size()),
        cli.debuginfod_connect_timeout
            .unwrap_or(defaults.connect_timeout()),
        cli.debuginfod_request_timeout
            .unwrap_or(defaults.request_timeout()),
    )
}

pub(crate) fn load_profiles_limits_overrides_config(
    path: Option<&Path>,
) -> Result<OverridesProvider, Box<dyn std::error::Error>> {
    let Some(path) = path else {
        return Ok(OverridesProvider::new(Limits::default()));
    };
    let text = std::fs::read_to_string(path)?;
    Ok(OverridesProvider::from_yaml(&text)?)
}
