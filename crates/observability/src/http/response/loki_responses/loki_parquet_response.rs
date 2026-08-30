use super::{
    HttpQueryError, LokiMetricParquetKind, Response, Value, loki_metrics_parquet_response,
    loki_streams_parquet_response,
};

pub(crate) fn loki_parquet_response(value: &Value) -> Result<Response, HttpQueryError> {
    match value.pointer("/data/resultType").and_then(Value::as_str) {
        Some("streams") => loki_streams_parquet_response(value),
        Some("matrix") => loki_metrics_parquet_response(value, LokiMetricParquetKind::Matrix),
        Some("vector") => loki_metrics_parquet_response(value, LokiMetricParquetKind::Vector),
        _ => Err(HttpQueryError::LokiParquet(
            "only stream and metric query results can be encoded as parquet",
        )),
    }
}
