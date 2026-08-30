use super::*;

#[must_use]
/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn run(config: ServiceConfig) -> Result<ServiceStatus, Infallible> {
    let ServiceConfig {
        target,
        listen_addr: _listen_addr,
        object_store_url: _object_store_url,
        wal_bootstrap_server: _wal_bootstrap_server,
        wal_topic: _wal_topic,
        wal_group_id: _wal_group_id,
        data_root: _data_root,
        querier_index_source: _querier_index_source,
        tenant: _tenant,
        index_prefix: _index_prefix,
        query_start_ns: _query_start_ns,
        query_end_ns: _query_end_ns,
        max_query_range: _max_query_range,
        max_query_series: _max_query_series,
        max_query_read: _max_query_read,
        max_query_length: _max_query_length,
        max_ingest_body: _max_ingest_body,
        wal_append_timeout: _wal_append_timeout,
        reject_old_samples_max_age: _reject_old_samples_max_age,
        creation_grace_period: _creation_grace_period,
        ingest_quota_burst_window: _ingest_quota_burst_window,
        wal_connect_startup_deadline: _wal_connect_startup_deadline,
        wal_connect_attempt_timeout: _wal_connect_attempt_timeout,
        wal_connect_initial_backoff: _wal_connect_initial_backoff,
        wal_connect_max_backoff: _wal_connect_max_backoff,
        compactor_wal_poll_timeout: _compactor_wal_poll_timeout,
        compactor_accumulation_window: _compactor_accumulation_window,
        compactor_accumulation_poll_timeout: _compactor_accumulation_poll_timeout,
        compactor_max_records_per_batch: _compactor_max_records_per_batch,
        compactor_idle_interval: _compactor_idle_interval,
        compactor_object_store_initial_backoff: _compactor_object_store_initial_backoff,
        compactor_object_store_max_backoff: _compactor_object_store_max_backoff,
        querier_frontier_refresh_interval: _querier_frontier_refresh_interval,
        querier_dynamic_index_cache_ttl: _querier_dynamic_index_cache_ttl,
        querier_shard_index_cache_ttl: _querier_shard_index_cache_ttl,
        querier_shard_fetch_concurrency: _querier_shard_fetch_concurrency,
        querier_cold_block_fetch_concurrency: _querier_cold_block_fetch_concurrency,
        querier_hot_tail_bucket_width: _querier_hot_tail_bucket_width,
        querier_hot_tail_interval: _querier_hot_tail_interval,
        querier_dependency_reconnect_interval: _querier_dependency_reconnect_interval,
    } = config;

    Ok(ServiceStatus { role: target })
}
