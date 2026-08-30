use super::*;

pub(crate) fn range_cache_key_object_name(tenant: &str, query: &FrontendRangeQuery) -> String {
    let mut key = String::new();
    append_hex_component(&mut key, tenant.as_bytes());
    key.push('/');
    append_hex_component(&mut key, query.query.as_bytes());
    let shard = query
        .shard
        .map_or_else(|| "none".to_string(), QueryShard::selector_value);
    let _ = write!(
        key,
        "/{}-{}-{}-{}-{}",
        query.start_ms,
        query.end_ms,
        query.step.millis_i64(),
        query.shard.map_or(0, |shard| shard.index),
        query.shard.map_or(0, |shard| shard.total)
    );
    key.push('-');
    append_hex_component(&mut key, shard.as_bytes());
    key
}
