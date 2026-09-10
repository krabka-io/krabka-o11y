use super::{
    Arc, BTreeSet, BlockMeta, BlockStoreError, BlockStreamWriter, BlockWriter,
    DEFAULT_BLOCK_READ_MAX, DownsamplePolicy, MERGE_BATCH_ROWS, MERGE_READ_BATCH_ROWS, ObjectStore,
    ObjectStoreExt, Path, ProfileIndex, ProfilesError, PutPayload, RecordBatch, SampleGroupBuffer,
    SchemaRef, SortedMerge, StreamExt, SummaryColumns, SymbolDb, destination_partitions,
    downsample_batches, load_symdb, open_block_stream, profile_samples_decl, remap_partitions,
    source_partitions, versioned_compaction_key,
};

/// Merges profile blocks into one, optionally summing their samples into
/// coarser time buckets.
///
/// The inputs are each already in the declared
/// `[fingerprint, profile_type, timestamp]` order, so they are merged rather
/// than concatenated and sorted: the merge holds one batch of each input, and
/// the block writer takes the merged batches in the order it wants them. What
/// stays resident is the output's symbol DB and one batch per input, not the
/// samples of every block being read.
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

    let mut out_symbols = SymbolDb::new();
    let mut out_partitions = BTreeSet::new();
    let mut schema = None;
    let mut input_versions = Vec::with_capacity(input_keys.len());
    let mut runs = Vec::with_capacity(input_keys.len());

    for (block_idx, block_key) in input_keys.iter().enumerate() {
        let source_partitions = source_partitions(index, block_key);
        let partition_map = destination_partitions(block_idx, &source_partitions)?;
        let (symdb_meta, symdb) = load_symdb(store, block_key).await?;
        input_versions.push(symdb_meta);
        for (source, dest) in &partition_map {
            out_symbols
                .copy_partition_from(&symdb, *source, *dest)
                .map_err(|err| ProfilesError::Block(err.to_string()))?;
            out_partitions.insert(*dest);
        }

        let (meta, block_schema, batches) = open_block_stream(
            Arc::clone(store),
            block_key,
            DEFAULT_BLOCK_READ_MAX,
            MERGE_READ_BATCH_ROWS,
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
        input_versions.push(meta);
        validate_compaction_schema(&mut schema, block_schema, block_key)?;
        runs.push(
            batches
                .map(move |batch| {
                    remap_partitions(&batch?, &partition_map)
                        .map_err(|err| BlockStoreError::InvalidBlock(err.to_string()))
                })
                .boxed(),
        );
    }

    let schema =
        schema.ok_or_else(|| ProfilesError::Block("cannot compact empty block set".into()))?;
    let decl = profile_samples_decl();
    let output_key = versioned_compaction_key(output_key, &input_versions);
    let mut merge = SortedMerge::new(schema.clone(), &decl.sort_key, runs, MERGE_BATCH_ROWS)
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    let mut block = BlockWriter::new(Arc::clone(store))
        .open_block(
            tenant,
            &output_key,
            schema.clone(),
            &decl,
            SummaryColumns::series(),
        )
        .map_err(|err| ProfilesError::Block(err.to_string()))?;

    let writes: Result<(), ProfilesError> = async {
        match downsample {
            None => {
                while let Some(merged) = next_merged(&mut merge).await? {
                    write(&mut block, &merged).await?;
                }
            }
            Some(policy) => {
                if policy.resolution_ns <= 0 {
                    return Err(ProfilesError::Block(
                        "downsample resolution must be positive".to_string(),
                    ));
                }
                let mut buffer = SampleGroupBuffer::new(schema, policy.resolution_ns);
                while let Some(merged) = next_merged(&mut merge).await? {
                    buffer.push(merged);
                    while let Some(complete) = buffer.take_complete(MERGE_BATCH_ROWS)? {
                        write_downsampled(&mut block, &complete, policy).await?;
                    }
                }
                if let Some(rest) = buffer.take_rest()? {
                    write_downsampled(&mut block, &rest, policy).await?;
                }
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = writes {
        block
            .abort()
            .await
            .map_err(|err| ProfilesError::Block(err.to_string()))?;
        return Err(error);
    }

    let mut meta = block
        .finish()
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;
    store
        .put(
            &Path::from(format!("{output_key}.symdb")),
            PutPayload::from(out_symbols.encode()),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))?;

    // The index derives the level from the blocks being retired and hands it
    // back, so the meta this returns says what the index recorded rather than
    // the level-zero the writer stamped.
    meta.level = index.replace_profile_blocks(
        tenant,
        input_keys,
        &[(meta.clone(), out_partitions.into_iter().collect())],
    );
    Ok(meta)
}

fn validate_compaction_schema(
    schema: &mut Option<SchemaRef>,
    candidate: SchemaRef,
    block_key: &str,
) -> Result<(), ProfilesError> {
    if let Some(expected) = schema {
        if expected.as_ref() != candidate.as_ref() {
            return Err(ProfilesError::Block(format!(
                "profile block `{block_key}` has a different schema from the first compaction input"
            )));
        }
    } else {
        *schema = Some(candidate);
    }
    Ok(())
}
async fn next_merged(merge: &mut SortedMerge) -> Result<Option<RecordBatch>, ProfilesError> {
    merge
        .next_batch()
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))
}

async fn write(block: &mut BlockStreamWriter, batch: &RecordBatch) -> Result<(), ProfilesError> {
    block
        .write_batch(batch)
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))
}

/// Sums a run of complete time buckets and writes the result.
async fn write_downsampled(
    block: &mut BlockStreamWriter,
    batch: &RecordBatch,
    policy: DownsamplePolicy,
) -> Result<(), ProfilesError> {
    for summed in downsample_batches(std::slice::from_ref(batch), policy)? {
        write(block, &summed).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn compaction_rejects_an_input_with_a_different_schema() {
        let first = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let different = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::UInt64,
            false,
        )]));
        let mut selected = None;

        validate_compaction_schema(&mut selected, first, "a.parquet").unwrap();
        let result = validate_compaction_schema(&mut selected, different, "b.parquet");

        assert2::assert!(
            matches!(result, Err(ProfilesError::Block(message)) if message.contains("b.parquet"))
        );
    }
}
