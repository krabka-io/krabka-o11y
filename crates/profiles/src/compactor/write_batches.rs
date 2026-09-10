use super::{
    Arc, BlockMeta, BlockWriter, ObjectStore, ProfilesError, RecordBatch, SummaryColumns,
    profile_samples_decl,
};

/// Writes the compacted batches as one profile block and returns its metadata.
///
/// Going through [`BlockWriter`] is what gives the compacted block the same
/// physical shape as a freshly built one -- validated against
/// [`profile_samples_decl`], in the declared sort order, compressed, cut into
/// row groups, and streamed rather than buffered whole -- and what makes its
/// [`BlockMeta`] a summary of the columns actually written instead of a tally
/// kept beside them.
pub(crate) async fn write_batches(
    store: &Arc<dyn ObjectStore>,
    tenant: &str,
    output_key: &str,
    batches: &[RecordBatch],
) -> Result<BlockMeta, ProfilesError> {
    let Some(first) = batches.first() else {
        return Err(ProfilesError::Block(
            "cannot compact empty block set".to_string(),
        ));
    };
    BlockWriter::new(Arc::clone(store))
        .write_block_with_decl(
            tenant,
            output_key,
            first.schema(),
            batches,
            &profile_samples_decl(),
            SummaryColumns::series(),
        )
        .await
        .map_err(|err| ProfilesError::Block(err.to_string()))
}
