use clap::Parser as _;

use super::prelude::{
    Arc, BlockIndex, BufferedLogHotTail, ClientResourcePolicy, CompactionFrontierSource,
    InMemoryWalSink, IngestLimitError, LabelIndex, LogIngestLimiter, LogQueryAuthorizer,
    QuerierState, QueryAuthorizationError, ServiceConfig, ServiceDependencies, ServiceMetrics,
    SharedCompactionFrontier, StreamScanOptions, WalLogRecord, admin_connection_options,
    async_trait, build_service_dependencies, check, millis, minutes,
    next_compactor_object_store_backoff, secs, validate_compactor_policy,
    validate_distributor_policy,
};

mod compactor_policy_rejects_zero_and_invalid_bounds;
mod compactor_policy_uses_defaults_and_cli_overrides;
mod distributor_dependency_startup_rejects_invalid_policy_before_connecting;
mod distributor_policy_rejects_zero_and_invalid_bounds;
mod querier_policy_rejects_zero;
mod querier_policy_uses_defaults_and_cli_overrides;
mod retry_backoff_doubles_and_caps;
mod service_dependencies_builder_methods_preserve_existing_fields;
