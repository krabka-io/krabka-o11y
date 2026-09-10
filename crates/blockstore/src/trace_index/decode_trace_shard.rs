use super::{
    BTreeMap, BTreeSet, BlockLevel, BlockStoreError, BloomShard, ByteReader, Result,
    ShardedTraceBloom, TRACE_SHARD_FORMAT_VERSION, TRACE_SHARD_MAGIC, TraceBlockStats,
};

/// Decodes one shard payload into the tenant it names and its block records.
///
/// Every length in the encoding is checked against the bytes that remain, and
/// every dictionary id against the dictionary, so a truncated or scrambled
/// object fails here rather than producing an index that answers queries
/// wrongly. `object_key` names the object in the error.
///
/// # Errors
/// Returns [`BlockStoreError::InvalidBlock`] when the bytes are not a trace
/// shard of the current format version, when they end early, or when a bloom
/// they carry could not be used without panicking.
pub(crate) fn decode_trace_shard(
    object_key: &str,
    bytes: &[u8],
) -> Result<(String, Vec<TraceBlockStats>)> {
    decode(bytes).map_err(|error| match error {
        BlockStoreError::InvalidBlock(message) => {
            BlockStoreError::InvalidBlock(format!("trace index shard `{object_key}`: {message}"))
        }
        other => other,
    })
}

fn decode(bytes: &[u8]) -> Result<(String, Vec<TraceBlockStats>)> {
    let mut reader = ByteReader::new(bytes);
    if reader.take(TRACE_SHARD_MAGIC.len(), "the magic")? != TRACE_SHARD_MAGIC {
        return Err(BlockStoreError::InvalidBlock(
            "is not a trace index shard".to_string(),
        ));
    }
    let version = reader.u8("the format version")?;
    if version != TRACE_SHARD_FORMAT_VERSION {
        return Err(BlockStoreError::InvalidBlock(format!(
            "is format version {version}, expected {TRACE_SHARD_FORMAT_VERSION}"
        )));
    }

    let tenant = reader.string("the tenant name")?;

    let dictionary_len = reader.count("dictionary entries")?;
    let mut dictionary = Vec::with_capacity(dictionary_len);
    for _ in 0..dictionary_len {
        dictionary.push(reader.string("a dictionary entry")?);
    }

    let block_count = reader.count("blocks")?;
    let mut blocks = Vec::with_capacity(block_count);
    let mut previous_min = 0_i64;
    for _ in 0..block_count {
        let object_key = reader.string("a block object key")?;
        let min_ts = previous_min.wrapping_add(reader.ivarint("a block start")?);
        let max_ts = min_ts.wrapping_add(reader.ivarint("a block span")?);
        previous_min = min_ts;
        let row_count = reader.count_unbounded("a block row count")?;
        let level = BlockLevel(
            u32::try_from(reader.uvarint("a block level")?).map_err(|_| {
                BlockStoreError::InvalidBlock("names a compaction level beyond u32".to_string())
            })?,
        );

        let tag_name_count = reader.count("tag names of a block")?;
        let mut tag_names = BTreeSet::new();
        for _ in 0..tag_name_count {
            tag_names.insert(entry(&dictionary, reader.uvarint("a tag name id")?)?.to_string());
        }

        let tag_count = reader.count("tag value sets of a block")?;
        let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for _ in 0..tag_count {
            let tag = entry(&dictionary, reader.uvarint("a tag name id")?)?.to_string();
            let value_count = reader.count("values of a tag")?;
            let mut values = BTreeSet::new();
            for _ in 0..value_count {
                values.insert(entry(&dictionary, reader.uvarint("a tag value id")?)?.to_string());
            }
            tag_values.insert(tag, values);
        }

        let shard_count = reader.count("bloom shards of a block")?;
        let mut shards = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let num_bits = reader.uvarint("a bloom shard width")?;
            let k = u32::try_from(reader.uvarint("a bloom shard hash count")?).map_err(|_| {
                BlockStoreError::InvalidBlock("names a bloom hash count beyond u32".to_string())
            })?;
            let word_count = reader.count("bloom shard words")?;
            // A word is eight bytes, so the reader's own bound of "no more
            // elements than bytes left" would still let a corrupt count
            // reserve eight times the object. Grow instead of reserving.
            let mut bits = Vec::new();
            for _ in 0..word_count {
                bits.push(reader.u64_le("a bloom shard word")?);
            }
            shards.push(BloomShard { bits, num_bits, k });
        }
        let bloom = ShardedTraceBloom { shards };
        // `Deserialize` and this decoder both bypass the bloom constructors'
        // invariant checks, so a structurally-valid-but-corrupt payload would
        // panic on the first lookup. Reject it here instead.
        bloom.validate().map_err(|error| {
            BlockStoreError::InvalidBlock(format!(
                "carries a corrupt bloom for `{object_key}`: {error}"
            ))
        })?;

        blocks.push(TraceBlockStats {
            object_key,
            min_ts,
            max_ts,
            bloom,
            tag_names,
            tag_values,
            row_count,
            level,
        });
    }

    if !reader.is_exhausted() {
        return Err(BlockStoreError::InvalidBlock(
            "has bytes after the end of the shard".to_string(),
        ));
    }

    Ok((tenant, blocks))
}

fn entry(dictionary: &[String], id: u64) -> Result<&str> {
    usize::try_from(id)
        .ok()
        .and_then(|id| dictionary.get(id))
        .map(String::as_str)
        .ok_or_else(|| {
            BlockStoreError::InvalidBlock(format!(
                "names dictionary entry {id}, which is not there"
            ))
        })
}
