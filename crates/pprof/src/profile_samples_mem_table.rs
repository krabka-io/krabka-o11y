use arrow::record_batch::RecordBatch;
use datafusion::{catalog::MemTable, common::Result};

use crate::profile_samples_schema;

/// Builds a query table for profile samples, giving an empty scan its schema.
pub fn profile_samples_mem_table(mut batches: Vec<RecordBatch>) -> Result<MemTable> {
    if batches.is_empty() {
        batches.push(RecordBatch::new_empty(profile_samples_schema()));
    }
    MemTable::try_new(profile_samples_schema(), vec![batches])
}
