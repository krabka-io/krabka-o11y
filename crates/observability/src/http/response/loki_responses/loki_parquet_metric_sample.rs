use super::*;

pub(crate) fn loki_parquet_metric_sample(
    sample: &Value,
    kind: LokiMetricParquetKind,
) -> Result<(i64, f64), HttpQueryError> {
    let sample = sample
        .as_array()
        .ok_or(HttpQueryError::LokiParquet("metric sample is not an array"))?;
    let timestamp_ns = loki_parquet_metric_timestamp_ns(
        sample
            .first()
            .ok_or(HttpQueryError::LokiParquet("missing metric timestamp"))?,
        kind,
    )?;
    let value = sample
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
        .and_then(MetricValue::to_f64)
        .ok_or(HttpQueryError::LokiParquet("metric value is not numeric"))?;
    Ok((timestamp_ns, value))
}
