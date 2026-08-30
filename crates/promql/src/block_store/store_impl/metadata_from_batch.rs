use super::{Result, MetadataRecord, Array, StringArray, PromqlError};

pub(crate) fn metadata_from_batch(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<MetadataRecord>> {
    let names = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            PromqlError::Store("metadata metric_family_name column has wrong type".into())
        })?;
    let types = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("metadata metric_type column has wrong type".into()))?;
    let helps = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("metadata help column has wrong type".into()))?;
    let units = batch
        .column(5)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("metadata unit column has wrong type".into()))?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        out.push(MetadataRecord {
            metric_family_name: names.value(row).to_string(),
            metric_type: types.value(row).to_string(),
            help: helps.value(row).to_string(),
            unit: units.value(row).to_string(),
        });
    }
    Ok(out)
}
