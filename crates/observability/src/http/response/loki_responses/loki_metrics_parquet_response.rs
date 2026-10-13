use super::*;

pub(crate) fn loki_metrics_parquet_response(
    value: &Value,
    kind: LokiMetricParquetKind,
) -> Result<Response, HttpQueryError> {
    let results = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or(HttpQueryError::LokiParquet("missing metric result array"))?;
    let mut timestamps = Vec::new();
    let mut label_sets = Vec::new();
    let mut values = Vec::new();
    for series in results {
        let labels = loki_parquet_labels(series.get("metric"), "metric labels")?;
        match kind {
            LokiMetricParquetKind::Matrix => {
                let samples = series
                    .get("values")
                    .and_then(Value::as_array)
                    .ok_or(HttpQueryError::LokiParquet("missing matrix values array"))?;
                for sample in samples {
                    let point = loki_parquet_metric_sample(sample, kind)?;
                    timestamps.push(point.timestamp_ns);
                    label_sets.push(labels.clone());
                    values.push(point.value);
                }
            }
            LokiMetricParquetKind::Vector => {
                let sample = series
                    .get("value")
                    .ok_or(HttpQueryError::LokiParquet("missing vector value"))?;
                let point = loki_parquet_metric_sample(sample, kind)?;
                timestamps.push(point.timestamp_ns);
                label_sets.push(labels.clone());
                values.push(point.value);
            }
        }
    }

    let columns = loki_parquet_columns(timestamps, &label_sets)?;
    let value_array = Float64Array::from(values);
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", columns.timestamp_data_type, false),
        Field::new("labels", columns.labels_array.data_type().clone(), false),
        Field::new("value", DataType::Float64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(columns.timestamp_array) as ArrayRef,
            Arc::new(columns.labels_array) as ArrayRef,
            Arc::new(value_array) as ArrayRef,
        ],
    )?;
    loki_parquet_batch_response(&batch)
}
