use super::{ClockReadingRow, RecordBatch, HistogramCodecError, ClockColumns, clock_reading_schema, Arc};

/// Encodes sorted clock rows into a block against
/// [`clock_reading_schema`](crate::schema::clock_reading_schema).
pub(crate) fn encode_clock_reading_rows(rows: &[ClockReadingRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut columns = ClockColumns::new();
    for row in rows {
        columns.append(row);
    }

    let schema = clock_reading_schema();
    let mut named = columns.finish();
    named.sort_by_key(|(name, _)| schema.index_of(name).unwrap_or(usize::MAX));
    // `index_of` returns an error for a name the schema does not declare, and
    // the sort parks such a column last. Ask again here so the mismatch becomes
    // an error rather than a block with its columns in the wrong order.
    for (name, _) in &named {
        schema.index_of(name)?;
    }

    Ok(RecordBatch::try_new(
        Arc::clone(&schema),
        named.into_iter().map(|(_, array)| array).collect(),
    )?)
}
