use super::{
    BTreeMap, BlockEntry, BlockLevel, BlockList, BlockListRepr, BlockStoreError, ByteReader,
    INDEX_SHARD_FORMAT_VERSION, INDEX_SHARD_MAGIC, Index, Labels, Result, SeriesFingerprint,
    TenantIndex,
};

/// Decodes one shard into a single-tenant [`Index`].
///
/// Every length in the encoding is checked against the bytes that remain, and
/// every dictionary id against the dictionary, so a truncated or scrambled
/// object fails here rather than producing an index that answers queries
/// wrongly. `object_key` names the object in the error.
///
/// # Errors
/// Returns [`BlockStoreError::InvalidBlock`] when the bytes are not a shard of
/// the current format version, or when they end early.
pub(crate) fn decode_index_shard(object_key: &str, bytes: &[u8]) -> Result<Index> {
    decode(bytes).map_err(|error| match error {
        BlockStoreError::InvalidBlock(message) => {
            BlockStoreError::InvalidBlock(format!("index shard `{object_key}`: {message}"))
        }
        other => other,
    })
}

fn decode(bytes: &[u8]) -> Result<Index> {
    let mut reader = ByteReader::new(bytes);
    if reader.take(INDEX_SHARD_MAGIC.len(), "the magic")? != INDEX_SHARD_MAGIC {
        return Err(BlockStoreError::InvalidBlock(
            "is not an index shard".to_string(),
        ));
    }
    let version = reader.u8("the format version")?;
    if version != INDEX_SHARD_FORMAT_VERSION {
        return Err(BlockStoreError::InvalidBlock(format!(
            "is format version {version}, expected {INDEX_SHARD_FORMAT_VERSION}"
        )));
    }

    let tenant = reader.string("the tenant name")?;

    let dictionary_len = reader.count("dictionary entries")?;
    let mut dictionary = Vec::with_capacity(dictionary_len);
    for _ in 0..dictionary_len {
        dictionary.push(reader.string("a dictionary entry")?);
    }
    let mut tenant_index = TenantIndex::default();

    let series_count = reader.count("series")?;
    for _ in 0..series_count {
        let fingerprint: SeriesFingerprint = reader.u64_le("a series fingerprint")?;
        let label_count = reader.count("labels of a series")?;
        let mut labels = Labels::new();
        for _ in 0..label_count {
            let name = dictionary_entry(&dictionary, reader.uvarint("a label name id")?)?;
            let value = dictionary_entry(&dictionary, reader.uvarint("a label value id")?)?;
            labels.insert(name.to_string(), value.to_string());
        }
        tenant_index.add_series(fingerprint, &labels);
    }

    let block_count = reader.count("blocks")?;
    let mut entries = Vec::with_capacity(block_count);
    let mut previous_min = 0_i64;
    for _ in 0..block_count {
        let object_key = reader.string("a block object key")?;
        let min_ts = previous_min.wrapping_add(reader.ivarint("a block start")?);
        let max_ts = min_ts.wrapping_add(reader.ivarint("a block span")?);
        previous_min = min_ts;
        let row_count = reader.count_unbounded("a block row count")?;
        let fingerprint_count = reader.count_unbounded("a block fingerprint count")?;
        let fingerprint_digest = reader.u64_le("a block fingerprint digest")?;
        let level = BlockLevel(
            u32::try_from(reader.uvarint("a block level")?).map_err(|_| {
                BlockStoreError::InvalidBlock("names a compaction level beyond u32".to_string())
            })?,
        );
        entries.push(BlockEntry {
            object_key,
            min_ts,
            max_ts,
            row_count,
            fingerprint_count,
            fingerprint_digest,
            level,
        });
    }

    let posting_count = reader.count("postings")?;
    let mut postings: BTreeMap<SeriesFingerprint, Vec<u32>> = BTreeMap::new();
    let block_count = u32::try_from(entries.len()).expect("a checked block count fits a u32");
    for _ in 0..posting_count {
        let fingerprint: SeriesFingerprint = reader.u64_le("a posting fingerprint")?;
        let ordinal_count = reader.count("ordinals of a posting")?;
        let mut ordinals = Vec::with_capacity(ordinal_count);
        let mut previous = 0_u32;
        for _ in 0..ordinal_count {
            let delta = u32::try_from(reader.uvarint("a posting ordinal")?).map_err(|_| {
                BlockStoreError::InvalidBlock("names a block ordinal beyond the shard".to_string())
            })?;
            let ordinal = previous.checked_add(delta).ok_or_else(|| {
                BlockStoreError::InvalidBlock("names a block ordinal beyond the shard".to_string())
            })?;
            if ordinal >= block_count {
                return Err(BlockStoreError::InvalidBlock(format!(
                    "names block ordinal {ordinal} of {block_count}"
                )));
            }
            ordinals.push(ordinal);
            previous = ordinal;
        }
        postings.insert(fingerprint, ordinals);
    }

    if !reader.is_exhausted() {
        return Err(BlockStoreError::InvalidBlock(
            "has bytes after the end of the shard".to_string(),
        ));
    }

    tenant_index.blocks = BlockList::from(BlockListRepr { entries, postings });

    let mut index = Index::new();
    index.tenants.insert(tenant, tenant_index);
    Ok(index)
}

fn dictionary_entry(dictionary: &[String], id: u64) -> Result<&str> {
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
