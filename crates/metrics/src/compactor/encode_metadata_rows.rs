use super::{MetadataRow, RecordBatch, HistogramCodecError, UInt64Builder, Int64Builder, StringBuilder, ArrayRef, Arc, metadata_schema};

pub(crate) fn encode_metadata_rows(rows: &[MetadataRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut names = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut helps = StringBuilder::new();
    let mut units = StringBuilder::new();

    for row in rows {
        fingerprints.append_value(row.fingerprint);
        timestamps.append_value(0);
        names.append_value(&row.metric_family_name);
        types.append_value(&row.metric_type);
        helps.append_value(&row.help);
        units.append_value(&row.unit);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(names.finish()),
        Arc::new(types.finish()),
        Arc::new(helps.finish()),
        Arc::new(units.finish()),
    ];

    Ok(RecordBatch::try_new(metadata_schema(), columns)?)
}
