use super::QueryShardSelector;

/// # Errors
/// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
pub fn parse_query_shard_selector(value: &str) -> Result<QueryShardSelector, String> {
    let Some(separator) = value.find("_of_") else {
        return Err(format!("invalid query shard selector `{value}`"));
    };
    let index = value[..separator]
        .parse::<usize>()
        .map_err(|_| format!("invalid query shard selector `{value}`"))?;
    let total = value[separator + 4..]
        .parse::<usize>()
        .map_err(|_| format!("invalid query shard selector `{value}`"))?;
    if index == 0 || total == 0 || index > total {
        return Err(format!("invalid query shard selector `{value}`"));
    }
    Ok(QueryShardSelector { index, total })
}
