use super::{Future, WalBatchError};

/// Appends `records` one at a time, each waiting for its ack.
///
/// This is the batch append for a sink with no produce pipeline: it awaits
/// `append` for each record in order and stops at the first failure.
/// [`write_batch_pipelined`](super::write_batch_pipelined) is the windowed
/// form for a sink that can keep several sends in flight.
///
/// # Errors
/// Returns [`WalBatchError`] when a record fails to append. The error carries
/// the count of records that did append before it.
pub async fn write_batch_serially<Record, Error, Append, Appended>(
    records: Vec<Record>,
    append: Append,
) -> Result<(), WalBatchError<Error>>
where
    Append: Fn(Record) -> Appended,
    Appended: Future<Output = Result<(), Error>>,
    Error: std::error::Error + 'static,
{
    let total = records.len();
    for (appended, record) in records.into_iter().enumerate() {
        if let Err(source) = append(record).await {
            return Err(WalBatchError::new(appended, total, source));
        }
    }
    Ok(())
}
