use datafusion::arrow::{
    array::{Array as _, Int64Array, UInt64Array},
    record_batch::RecordBatch,
};

use super::{MapArray, QueryError, QueryRow, StringArray, structured_metadata_value};

/// Calls `visit` with every row of `batches`, which hold the columns a log
/// block scan selects: the series fingerprint, the timestamp, the line, and
/// the structured metadata.
pub(crate) fn for_each_query_row(
    batches: &[RecordBatch],
    mut visit: impl FnMut(QueryRow<'_>) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    for batch in batches {
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "series_fingerprint",
                expected: "UInt64",
            })?;
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "timestamp_ns",
                expected: "Int64",
            })?;
        let lines = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(QueryError::InvalidColumn {
                column: "line",
                expected: "Utf8",
            })?;
        let metadata = batch.column(3).as_any().downcast_ref::<MapArray>().ok_or(
            QueryError::InvalidColumn {
                column: "structured_metadata",
                expected: "Map<Utf8, Utf8>",
            },
        )?;

        for row in 0..batch.num_rows() {
            let structured_metadata = structured_metadata_value(metadata, row)?;
            visit(QueryRow {
                fingerprint: fingerprints.value(row),
                timestamp_ns: timestamps.value(row),
                line: lines.value(row),
                structured_metadata: &structured_metadata,
            })?;
        }
    }
    Ok(())
}
