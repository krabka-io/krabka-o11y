use super::*;

#[test]
pub(crate) fn querier_policy_uses_defaults_and_cli_overrides() {
    let defaults = ServiceConfig::default();
    check!(defaults.querier_frontier_refresh_interval == secs(5));
    check!(defaults.querier_dynamic_index_cache_ttl == secs(5));
    check!(defaults.querier_shard_index_cache_ttl == minutes(5));
    check!(defaults.querier_shard_fetch_concurrency.get() == 32);
    check!(defaults.querier_cold_block_fetch_concurrency.get() == 8);
    check!(defaults.querier_hot_tail_bucket_width == minutes(1));
    check!(defaults.querier_hot_tail_interval == millis(50));
    check!(defaults.querier_dependency_reconnect_interval == millis(500));

    let configured = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target=querier",
        "--querier-frontier-refresh-interval=6s",
        "--querier-dynamic-index-cache-ttl=7s",
        "--querier-shard-index-cache-ttl=6m",
        "--querier-shard-fetch-concurrency=33",
        "--querier-cold-block-fetch-concurrency=9",
        "--querier-hot-tail-bucket-width=2m",
        "--querier-hot-tail-interval=60ms",
        "--querier-dependency-reconnect-interval=600ms",
    ])
    .expect("valid querier policy");
    check!(configured.querier_frontier_refresh_interval == secs(6));
    check!(configured.querier_dynamic_index_cache_ttl == secs(7));
    check!(configured.querier_shard_index_cache_ttl == minutes(6));
    check!(configured.querier_shard_fetch_concurrency.get() == 33);
    check!(configured.querier_cold_block_fetch_concurrency.get() == 9);
    check!(configured.querier_hot_tail_bucket_width == minutes(2));
    check!(configured.querier_hot_tail_interval == millis(60));
    check!(configured.querier_dependency_reconnect_interval == millis(600));

    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_runtime_policy(&configured);
    check!(state.dynamic_index_cache.cache_ttl == secs(7));
    check!(state.dynamic_index_cache.shard_cache_ttl == minutes(6));
    check!(state.dynamic_index_cache.shard_fetch_concurrency.get() == 33);
    check!(state.cold_block_fetch_concurrency.get() == 9);
    check!(
        StreamScanOptions::exhaustive()
            .with_block_fetch_concurrency(state.cold_block_fetch_concurrency)
            .block_fetch_concurrency()
            == 9
    );
}
