use super::*;

pub(crate) fn exemplar_batch_from_rows(
    rows: &[(u64, i64, f64, &str, &str, &str, &str)],
) -> RecordBatch {
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

    for (fingerprint, timestamp_ms, value, trace_id, span_id, label_name, label_value) in rows {
        fingerprints.append_value(*fingerprint);
        timestamps.append_value(*timestamp_ms);
        values.append_value(*value);
        trace_ids.append_value(*trace_id);
        span_ids.append_value(*span_id);
        labels.keys().append_value(*label_name);
        labels.values().append_value(*label_value);
        labels.append(true).unwrap();
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(fingerprints.finish()),
        Arc::new(timestamps.finish()),
        Arc::new(values.finish()),
        Arc::new(trace_ids.finish()),
        Arc::new(span_ids.finish()),
        Arc::new(labels.finish()),
    ];
    RecordBatch::try_new(exemplar_schema(), columns).unwrap()
}
