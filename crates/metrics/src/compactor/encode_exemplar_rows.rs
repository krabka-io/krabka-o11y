use super::*;

pub(crate) fn encode_exemplar_rows(rows: &[ExemplarRow]) -> Result<RecordBatch, HistogramCodecError> {
    let mut fingerprints = UInt64Builder::new();
    let mut timestamps = Int64Builder::new();
    let mut values = Float64Builder::new();
    let mut trace_ids = StringBuilder::new();
    let mut span_ids = StringBuilder::new();
    let mut labels = MapBuilder::new(
        Some(arrow::array::builder::MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        StringBuilder::new(),
    )
    .with_values_field(Field::new("value", DataType::Utf8, false));

    for row in rows {
        fingerprints.append_value(row.fingerprint);
        timestamps.append_value(row.timestamp_ms);
        values.append_value(row.value);
        match &row.trace_id {
            Some(trace_id) => trace_ids.append_value(trace_id),
            None => trace_ids.append_null(),
        }
        match &row.span_id {
            Some(span_id) => span_ids.append_value(span_id),
            None => span_ids.append_null(),
        }
        for (name, value) in &row.labels {
            labels.keys().append_value(name);
            labels.values().append_value(value);
        }
        labels.append(true)?;
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
        Arc::new(trace_ids.finish()),
        Arc::new(span_ids.finish()),
        Arc::new(labels.finish()),
    ];

    Ok(RecordBatch::try_new(exemplar_schema(), columns)?)
}
