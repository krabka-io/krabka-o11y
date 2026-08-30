use super::{
    Arc, BTreeSet, BlockMeta, DownsamplePolicy, ObjectStore, ObjectStoreExt, Path, ProfileIndex,
    ProfilesError, PutPayload, SymbolDb, collect_meta, destination_partitions, downsample_batches,
    load_batches, load_symdb, remap_partitions, source_partitions, write_batches,
};

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn compact_blocks_with_policy(
    store: &Arc<dyn ObjectStore>,
    index: &mut ProfileIndex,
    tenant: &str,
    input_keys: &[String],
    output_key: &str,
    downsample: Option<DownsamplePolicy>,
) -> Result<BlockMeta, ProfilesError> {
    if input_keys.len() < 2 {
        return Err(ProfilesError::Block(
            "compaction requires at least two input blocks".to_string(),
        ));
    }

    let mut out_batches = Vec::new();
    let mut out_symbols = SymbolDb::new();
    let mut out_partitions = BTreeSet::new();
    let mut fingerprints = BTreeSet::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;

    for (block_idx, block_key) in input_keys.iter().enumerate() {
        let source_partitions = source_partitions(index, block_key);
        let partition_map = destination_partitions(block_idx, &source_partitions)?;
        let symdb = load_symdb(store, block_key).await?;
        for (source, dest) in &partition_map {
            out_symbols
                .copy_partition_from(&symdb, *source, *dest)
                .map_err(|err| ProfilesError::Block(err.to_string()))?;
            out_partitions.insert(*dest);
        }

        let batches = load_batches(store, block_key).await?;
        for batch in batches {
            let batch = remap_partitions(&batch, &partition_map)?;
            out_batches.push(batch);
        }
    }

    let out_batches = match downsample {
        Some(policy) => downsample_batches(&out_batches, policy)?,
        None => out_batches,
    };
    let mut row_count = 0_usize;
    for batch in &out_batches {
        collect_meta(batch, &mut fingerprints, &mut min_ts, &mut max_ts);
        row_count += batch.num_rows();
    }

    write_batches(store, output_key, &out_batches).await?;
    store
        .put(
            &Path::from(format!("{output_key}.symdb")),
            PutPayload::from(out_symbols.encode()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;

    let meta = BlockMeta {
        tenant: tenant.to_string(),
        object_key: output_key.to_string(),
        min_ts,
        max_ts,
        row_count,
        fingerprints: fingerprints.into_iter().collect(),
    };
    index.replace_profile_blocks(
        tenant,
        input_keys,
        &[(meta.clone(), out_partitions.into_iter().collect())],
    );
    Ok(meta)
}
