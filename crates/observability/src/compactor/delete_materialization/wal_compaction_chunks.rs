use super::WalLogRecord;

pub(crate) fn wal_compaction_chunks(records: Vec<WalLogRecord>) -> Vec<Vec<WalLogRecord>> {
    let mut chunks: Vec<Vec<WalLogRecord>> = Vec::new();
    for record in records {
        let Some(position) = record.position else {
            chunks.push(vec![record]);
            continue;
        };
        if let Some(chunk) = chunks.last_mut()
            && chunk.first().is_some_and(|first| {
                first.tenant == record.tenant
                    && first.position.is_some_and(|first_position| {
                        first_position.partition == position.partition
                    })
            })
        {
            chunk.push(record);
        } else {
            chunks.push(vec![record]);
        }
    }
    chunks
}
