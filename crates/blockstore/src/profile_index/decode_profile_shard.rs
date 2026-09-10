use super::{
    BTreeMap, BlockStoreError, ByteReader, PROFILE_SHARD_FORMAT_VERSION, PROFILE_SHARD_MAGIC,
    ProfileShard, Result, decode_index_shard,
};

/// Decodes one shard payload into the tenant it names and its slot of the
/// index.
///
/// Every length is checked against the bytes that remain, so a truncated or
/// scrambled object fails here rather than producing an index that answers
/// queries wrongly. `object_key` names the object in the error.
///
/// # Errors
/// Returns [`BlockStoreError::InvalidBlock`] when the bytes are not a profile
/// shard of the current format version, or when they end early.
pub(crate) fn decode_profile_shard(
    object_key: &str,
    bytes: &[u8],
) -> Result<(String, ProfileShard)> {
    decode(object_key, bytes).map_err(|error| match error {
        BlockStoreError::InvalidBlock(message) if !message.contains(object_key) => {
            BlockStoreError::InvalidBlock(format!("profile index shard `{object_key}`: {message}"))
        }
        other => other,
    })
}

fn decode(object_key: &str, bytes: &[u8]) -> Result<(String, ProfileShard)> {
    let mut reader = ByteReader::new(bytes);
    if reader.take(PROFILE_SHARD_MAGIC.len(), "the magic")? != PROFILE_SHARD_MAGIC {
        return Err(BlockStoreError::InvalidBlock(
            "is not a profile index shard".to_string(),
        ));
    }
    let version = reader.u8("the format version")?;
    if version != PROFILE_SHARD_FORMAT_VERSION {
        return Err(BlockStoreError::InvalidBlock(format!(
            "is format version {version}, expected {PROFILE_SHARD_FORMAT_VERSION}"
        )));
    }

    let inner_len = reader.count("the embedded index shard")?;
    let inner = reader.take(inner_len, "the embedded index shard")?;
    let index = decode_index_shard(object_key, inner)?;
    let tenant = index
        .tenant_names()
        .next()
        .ok_or_else(|| {
            BlockStoreError::InvalidBlock("embeds an index shard with no tenant".to_string())
        })?
        .clone();

    let partition_count = reader.count("stacktrace partition entries")?;
    let mut partitions: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for _ in 0..partition_count {
        let key = reader.string("a partitioned block object key")?;
        let count = reader.count("stacktrace partitions of a block")?;
        let mut values = Vec::new();
        for _ in 0..count {
            values.push(reader.uvarint("a stacktrace partition")?);
        }
        partitions.insert(key, values);
    }

    if !reader.is_exhausted() {
        return Err(BlockStoreError::InvalidBlock(
            "has bytes after the end of the shard".to_string(),
        ));
    }

    Ok((tenant, ProfileShard { index, partitions }))
}
