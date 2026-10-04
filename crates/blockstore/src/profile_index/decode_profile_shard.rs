use super::{
    BTreeMap, BlockStoreError, ByteReader, PROFILE_SHARD_FORMAT_VERSION, PROFILE_SHARD_MAGIC,
    ProfileShard, ProfileWalRange, Result, decode_index_shard,
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

    let len = reader.count("WAL range payload")?;
    let wal_ranges: BTreeMap<String, Vec<ProfileWalRange>> =
        serde_json::from_slice(reader.take(len, "WAL range payload")?)
            .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))?;
    if wal_ranges.values().flatten().any(|range| {
        range.partition < 0 || range.min_offset < 0 || range.max_offset < range.min_offset
    }) {
        return Err(BlockStoreError::InvalidBlock(
            "invalid profile WAL range".into(),
        ));
    }
    if !reader.is_exhausted() {
        return Err(BlockStoreError::InvalidBlock(
            "has bytes after the end of the shard".to_string(),
        ));
    }

    Ok((
        tenant,
        ProfileShard {
            index,
            partitions,
            wal_ranges,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        super::{PROFILE_SHARD_FORMAT_VERSION, encode_profile_shard},
        *,
    };
    use crate::Labels;

    #[test]
    fn wal_ranges_roundtrip_exactly_and_invalid_versions_or_offsets_are_rejected() {
        let mut shard = ProfileShard::default();
        shard
            .index
            .add_series("t", 1, &Labels::from_pairs([("app", "api")]));
        let expected = BTreeMap::from([(
            "block".to_string(),
            vec![ProfileWalRange {
                partition: 3,
                min_offset: i64::MAX - 2,
                max_offset: i64::MAX,
            }],
        )]);
        shard.wal_ranges = expected.clone();
        let bytes = encode_profile_shard("t", &shard);
        let (_, decoded) = decode_profile_shard("shard", &bytes).unwrap();
        assert2::assert!(decoded.wal_ranges == expected);
        for version in [
            0,
            PROFILE_SHARD_FORMAT_VERSION - 1,
            PROFILE_SHARD_FORMAT_VERSION + 1,
        ] {
            let mut invalid = bytes.clone();
            invalid[PROFILE_SHARD_MAGIC.len()] = version;
            assert2::assert!(decode_profile_shard("shard", &invalid).is_err());
        }
        shard.wal_ranges.values_mut().next().unwrap()[0].min_offset = -1;
        assert2::assert!(
            decode_profile_shard("shard", &encode_profile_shard("t", &shard)).is_err()
        );
    }
}
