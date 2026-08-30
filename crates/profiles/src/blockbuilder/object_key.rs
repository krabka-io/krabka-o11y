use super::*;

#[must_use]
pub fn object_key(
    tenant: &str,
    partition: i32,
    min_offset: i64,
    max_offset: i64,
    min_ts: i64,
    max_ts: i64,
) -> String {
    format!(
        "blocks/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{min_ts}-{max_ts}.parquet"
    )
}
