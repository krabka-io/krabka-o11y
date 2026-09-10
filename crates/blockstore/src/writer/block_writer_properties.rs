use super::{
    ArrowSchemaConverter, BLOCK_ROW_GROUP_ROWS, BLOCK_ZSTD_LEVEL, BlockSchema, BlockStoreError,
    ColumnPath, Compression, Result, SchemaDescriptor, SchemaRef, SortingColumn, WriterProperties,
    ZstdLevel,
};

/// Builds the Parquet writer properties a signal's block declaration implies.
///
/// Every knob here is derived rather than defaulted, because the parquet
/// defaults are wrong for a block on object storage in three separate ways:
/// they leave the data uncompressed, they put a whole block in one row group
/// so nothing can be pruned inside it, and they write no bloom filter for the
/// identity columns a point lookup searches by.
///
/// # Errors
/// Returns [`BlockStoreError::InvalidBlock`] when the declaration names a sort
/// or bloom column that `schema` does not carry as a top-level leaf, and
/// [`BlockStoreError::Parquet`] when the Arrow schema has no Parquet
/// equivalent.
pub fn block_writer_properties(schema: &SchemaRef, decl: &BlockSchema) -> Result<WriterProperties> {
    let descriptor = ArrowSchemaConverter::new().convert(schema)?;

    let sorting = decl
        .sort_key
        .iter()
        .map(|name| {
            Ok(SortingColumn {
                column_idx: leaf_index(&descriptor, name)?,
                descending: false,
                nulls_first: true,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let mut builder = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(BLOCK_ZSTD_LEVEL)?))
        .set_max_row_group_row_count(Some(BLOCK_ROW_GROUP_ROWS));

    if !sorting.is_empty() {
        builder = builder.set_sorting_columns(Some(sorting));
    }

    for name in &decl.bloom_columns {
        // Presence check only -- a bloom filter is addressed by column path,
        // not by leaf ordinal, but a declaration naming a column the schema
        // does not have is a bug worth failing on rather than ignoring.
        leaf_index(&descriptor, name)?;
        builder =
            builder.set_column_bloom_filter_enabled(ColumnPath::new(vec![name.clone()]), true);
    }

    Ok(builder.build())
}

/// The ordinal of top-level column `name` among the Parquet schema's leaves.
///
/// `SortingColumn::column_idx` indexes the row group's column chunks, which
/// are the leaves of the Parquet schema rather than the Arrow fields: one
/// nested Arrow column ahead of `name` contributes several. Asking the
/// converted descriptor is what keeps the two in step.
fn leaf_index(descriptor: &SchemaDescriptor, name: &str) -> Result<i32> {
    let found = descriptor
        .columns()
        .iter()
        .position(|column| column.path().parts() == [name]);
    let index = found.ok_or_else(|| {
        BlockStoreError::InvalidBlock(format!(
            "declared column `{name}` is not a top-level leaf of the block schema"
        ))
    })?;
    i32::try_from(index).map_err(|_| {
        BlockStoreError::InvalidBlock(format!("column `{name}` is past the Parquet leaf limit"))
    })
}
