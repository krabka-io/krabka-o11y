use clap::Parser as _;

use super::prelude::{
    AclEntry, AclOperation, Arc, BTreeMap, BTreeSet, BlockDescriptor, BlockIndex, BlockKey,
    CompactionFrontier, Duration, LOKI_REJECT_OLD_SAMPLES_MAX_AGE, LabelIndex, LogRow,
    LokiDirection, ObjectPath, PatternType, PermissionType, QuerierState, QueryHotTail,
    RecordingObjectStore, ResourceType, SeriesParams, ServiceConfig, StreamScanOptions, TimeRange,
    check, connect_with_startup_retry, current_unix_time_ns, days,
    execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options, hours,
    metadata_index_range, millis, minutes, parse_query, plan_stream_query, secs,
    write_log_block_to_object_store,
};

// --- FIX B3 tests ---

// === split-modules: generated submodules ===
mod acl_entry;
mod connect_with_startup_retry_gives_up_after_deadline;
mod connect_with_startup_retry_retries_then_succeeds;
mod connect_with_startup_retry_succeeds_on_first_try;
mod distributor_policy_uses_defaults_and_cli_overrides;
mod metadata_index_range_defaults_empty_metadata_requests_to_recent_window;
mod missing_timestamp_fallback_age_is_exact;
mod object_store_stream_query_batches_cold_block_reads;
mod querier_state_with_request_tenant_index_caches_shard_indexes_for_repeated_range;
mod querier_state_with_request_tenant_index_lists_shards_from_query_window_offset;
mod querier_state_with_request_tenant_index_reuses_shard_indexes_for_moving_ranges;

pub(crate) use acl_entry::acl_entry;
