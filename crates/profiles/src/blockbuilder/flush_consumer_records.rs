use super::*;

///
/// # Errors
/// Returns an error when the query is invalid, required profile data is malformed, or the backing profile store cannot satisfy the request.
pub async fn flush_consumer_records(
    store: &Arc<dyn ObjectStore>,
    records: &[ConsumerRecord],
    flush_records: usize,
) -> Result<Vec<BlockMeta>, ProfilesError> {
    let mut index = ProfileIndex::new();
    flush_consumer_records_with_index(store, &mut index, records, flush_records).await
}
