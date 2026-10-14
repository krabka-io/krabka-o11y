use super::{
    Arc, Float64Array, HttpQueryError, LokiMetricParquetKind, Response, Value,
    loki_parquet_columns, loki_parquet_labels, loki_parquet_metric_sample,
    loki_parquet_value_response,
};

fn append_metric_sample(
    sample: &Value,
    kind: LokiMetricParquetKind,
    labels: &[krabka_blockstore::LabelPair],
    timestamps: &mut Vec<i64>,
    label_sets: &mut Vec<Vec<krabka_blockstore::LabelPair>>,
    values: &mut Vec<f64>,
) -> Result<(), HttpQueryError> {
    let point = loki_parquet_metric_sample(sample, kind)?;
    timestamps.push(point.timestamp_ns);
    label_sets.push(labels.to_vec());
    values.push(point.value);
    Ok(())
}

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
                    append_metric_sample(
                        sample,
                        kind,
                        &labels,
                        &mut timestamps,
                        &mut label_sets,
                        &mut values,
                    )?;
                }
            }
            LokiMetricParquetKind::Vector => {
                let sample = series
                    .get("value")
                    .ok_or(HttpQueryError::LokiParquet("missing vector value"))?;
                append_metric_sample(
                    sample,
                    kind,
                    &labels,
                    &mut timestamps,
                    &mut label_sets,
                    &mut values,
                )?;
            }
        }
    }

    let columns = loki_parquet_columns(timestamps, &label_sets)?;
    loki_parquet_value_response(columns, "value", Arc::new(Float64Array::from(values)))
}
