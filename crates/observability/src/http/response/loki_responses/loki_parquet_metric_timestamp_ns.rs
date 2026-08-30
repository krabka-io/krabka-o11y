use super::*;

pub(crate) fn loki_parquet_metric_timestamp_ns(
    value: &Value,
    kind: LokiMetricParquetKind,
) -> Result<i64, HttpQueryError> {
    if matches!(kind, LokiMetricParquetKind::Vector)
        && let Some(timestamp_ns) = value.as_i64()
    {
        return Ok(timestamp_ns);
    }

    if let Some(seconds) = value.as_i64() {
        return seconds
            .checked_mul(1_000_000_000)
            .ok_or(HttpQueryError::LokiParquet(
                "metric timestamp is out of range",
            ));
    }
    let seconds = value.as_f64().ok_or(HttpQueryError::LokiParquet(
        "metric timestamp is not numeric",
    ))?;
    let timestamp_ns = (seconds * 1_000_000_000.0).round();
    i64::from_f64(timestamp_ns).ok_or(HttpQueryError::LokiParquet(
        "metric timestamp is out of range",
    ))
}
