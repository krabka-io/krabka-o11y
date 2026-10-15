//! Column contract for the profile samples table.

use arrow::record_batch::RecordBatch;
use datafusion::{catalog::MemTable, common::Result};

pub use krabka_blockstore::{
    COL_FINGERPRINT, COL_TIMESTAMP, PCOL_PROFILE_TYPE, PCOL_SPAN_ID, PCOL_STACKTRACE_ID,
    PCOL_STACKTRACE_PARTITION, PCOL_TOTAL_VALUE, PCOL_TRACE_ID, PCOL_VALUE, PCOL_WAL_SAMPLE_IDS,
    profile_samples_schema,
};

/// Builds a query table for profile samples, giving an empty scan its schema.
///
/// # Errors
///
/// Returns an error if the batches have incompatible schemas.
pub fn profile_samples_mem_table(mut batches: Vec<RecordBatch>) -> Result<MemTable> {
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(profile_samples_schema()));
    }
    MemTable::try_new(profile_samples_schema(), vec![batches])
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use assert2::assert;

    use super::*;

    #[yare::parameterized(
        stacktrace_partition = { PCOL_STACKTRACE_PARTITION, DataType::UInt64 },
        stacktrace_id = { PCOL_STACKTRACE_ID, DataType::UInt64 },
        value = { PCOL_VALUE, DataType::Int64 },
    )]
    fn samples_schema_has_fold_keys_and_value(column: &'static str, want: DataType) {
        let schema = profile_samples_schema();
        assert2::assert!(
            schema.field_with_name(column).unwrap().data_type() == &want,
            "{column}"
        );
    }

    #[test]
    fn samples_schema_has_optional_trace_id() {
        let schema = profile_samples_schema();
        let field = schema.field_with_name(PCOL_TRACE_ID).unwrap();
        assert!(field.is_nullable() && field.data_type() == &DataType::Binary);
    }
}
