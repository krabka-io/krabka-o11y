use super::*;

pub(crate) async fn append_wal_records(
    sink: &dyn LogWalSink,
    records: Vec<WalLogRecord>,
) -> Result<(), WalSinkError> {
    for record in records {
        sink.append(record).await?;
    }
    Ok(())
}
