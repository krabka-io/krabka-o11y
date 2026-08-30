use super::*;

/// Arrow batches produced from one tenant's compacted rows.
pub struct TenantBatches {
    pub float: Option<RecordBatch>,
    pub native_histograms: Option<RecordBatch>,
    pub exemplars: Option<RecordBatch>,
    pub metadata: Option<RecordBatch>,
    pub clock_readings: Option<RecordBatch>,
}
