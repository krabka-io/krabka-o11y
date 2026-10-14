use clap::Parser as _;
use krabka_units::convert::{ByteSizeExt as _, TimeExt as _};

use super::prelude::{
    Arc, BlockIndex, BufferedLogHotTail, ClientResourcePolicy, CompactionFrontierSource,
    DeferredWalConsumerConnect, InMemoryWalSink, IngestLimitError, LabelIndex, LogIngestLimiter,
    LogQueryAuthorizer, Principal, QuerierState, QueryAuthorizationError, ServiceConfig,
    ServiceDependencies, ServiceMetrics, SharedCompactionFrontier, StreamScanOptions, TenantId,
    WalLogRecord, admin_connection_options, async_trait, build_service_dependencies, check, days,
    hours, mebibytes, millis, minutes, next_compactor_object_store_backoff, secs,
    validate_compactor_policy, validate_distributor_policy, with_querier_dependencies,
};

mod compactor_policy_rejects_zero_and_invalid_bounds;
mod compactor_policy_uses_defaults_and_cli_overrides;
mod distributor_dependency_startup_rejects_invalid_policy_before_connecting;
mod distributor_policy_rejects_zero_and_invalid_bounds;
mod querier_policy_rejects_zero;
mod querier_policy_uses_defaults_and_cli_overrides;
mod retry_backoff_doubles_and_caps;
mod service_dependencies_builder_methods_preserve_existing_fields;
mod wal_client_security_flags_reach_every_wal_connection;
