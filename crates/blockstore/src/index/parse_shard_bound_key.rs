/// Reads back a bound written by [`super::shard_bound_key`].
pub(crate) fn parse_shard_bound_key(key: &str) -> Option<i64> {
    if key.len() != 20 || !key.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let ordered = key.parse::<u64>().ok()?;
    Some(i64::from_le_bytes((ordered ^ (1 << 63)).to_le_bytes()))
}
