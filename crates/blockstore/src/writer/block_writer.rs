use super::*;

/// Writes Parquet blocks to an object store.
pub struct BlockWriter {
    pub(crate) store: Arc<dyn ObjectStore>,
}

impl BlockWriter {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    /// Writes `batches` as a single Parquet block at `object_key`.
    ///
    /// Returns [`BlockMeta`] computed from the mandatory block columns.
    ///
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn write_block(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<BlockMeta> {
        self.write_block_with_decl(
            tenant,
            object_key,
            schema,
            batches,
            &series_block_schema(),
            SummaryColumns::series(),
        )
        .await
    }

    /// Writes a block validated against a signal-specific schema declaration.
    ///
    /// Returns [`BlockMeta`] computed from the declared summary columns.
    #[instrument(
        skip_all,
        fields(tenant = %tenant, object_key = %object_key, batches = batches.len()),
        err
    )]
    /// # Errors
    /// Returns an error when object-store I/O fails, persisted metadata is malformed, or a block cannot be encoded or decoded.
    pub async fn write_block_with_decl(
        &self,
        tenant: &str,
        object_key: &str,
        schema: SchemaRef,
        batches: &[RecordBatch],
        decl: &BlockSchema,
        summary: SummaryColumns,
    ) -> Result<BlockMeta> {
        validate_against(&schema, decl)?;
        validate_batch_schemas(&schema, batches)?;

        let (min_ts, max_ts, row_count, fingerprints) = summarize(batches, &summary)?;

        let path = Path::from(object_key);
        let object_writer = BufWriter::new(self.store.clone(), path);
        let mut writer = AsyncArrowWriter::try_new(object_writer, schema, None)?;
        for batch in batches {
            writer.write(batch).await?;
        }
        writer.close().await?;

        Ok(BlockMeta {
            tenant: tenant.to_string(),
            object_key: object_key.to_string(),
            min_ts,
            max_ts,
            row_count,
            fingerprints,
        })
    }
}
