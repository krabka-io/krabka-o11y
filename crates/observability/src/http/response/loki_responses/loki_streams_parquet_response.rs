use super::*;

pub(crate) fn loki_streams_parquet_response(value: &Value) -> Result<Response, HttpQueryError> {
    let results = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or(HttpQueryError::LokiParquet("missing stream result array"))?;
    let mut timestamps = Vec::new();
    let mut label_sets = Vec::new();
    let mut lines = Vec::new();
    for stream in results {
        let labels = loki_parquet_labels(stream.get("stream"), "stream labels")?;
        let values = stream
            .get("values")
            .and_then(Value::as_array)
            .ok_or(HttpQueryError::LokiParquet("missing stream values array"))?;
        for entry in values {
            let entry = entry
                .as_array()
                .ok_or(HttpQueryError::LokiParquet("stream value is not an array"))?;
            let timestamp = entry
                .first()
                .and_then(Value::as_str)
                .ok_or(HttpQueryError::LokiParquet(
                    "stream timestamp is not a string",
                ))?
                .parse::<i64>()
                .map_err(|_| HttpQueryError::LokiParquet("stream timestamp is not an integer"))?;
            let line = entry
                .get(1)
                .and_then(Value::as_str)
                .ok_or(HttpQueryError::LokiParquet("stream line is not a string"))?;
            timestamps.push(timestamp);
            label_sets.push(labels.clone());
            lines.push(line.to_string());
        }
    }

    let timestamp_data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
    let timestamp_array =
        TimestampNanosecondArray::from(timestamps).with_data_type(timestamp_data_type.clone());
    let labels_array = loki_parquet_label_array(&label_sets)?;
    let line_array = StringArray::from(lines);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", timestamp_data_type, false),
        Field::new("labels", labels_array.data_type().clone(), false),
        Field::new("line", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_array) as ArrayRef,
            Arc::new(labels_array) as ArrayRef,
            Arc::new(line_array) as ArrayRef,
        ],
    )?;
    loki_parquet_batch_response(&batch)
}
