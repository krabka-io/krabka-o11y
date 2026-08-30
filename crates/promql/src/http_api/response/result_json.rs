use super::{
    QueryResult, SampleValue, Value, json, labels_json, native_histogram_json, range_matrix_json,
    sample_string, timestamp_seconds,
};

pub(crate) fn result_json(result: QueryResult) -> Value {
    match result {
        QueryResult::Scalar { ts_ms, value } => json!({
            "resultType": "scalar",
            "result": [timestamp_seconds(ts_ms), sample_string(value)],
        }),
        QueryResult::InstantVector(samples) => {
            let result = samples
                .into_iter()
                .map(|sample| match sample.value {
                    SampleValue::Float(value) => json!({
                        "metric": labels_json(&sample.labels),
                        "value": [timestamp_seconds(sample.ts_ms), sample_string(value)],
                    }),
                    SampleValue::Histogram(histogram) => json!({
                        "metric": labels_json(&sample.labels),
                        "histogram": [timestamp_seconds(sample.ts_ms), native_histogram_json(&histogram)],
                    }),
                })
                .collect::<Vec<_>>();
            json!({
                "resultType": "vector",
                "result": result,
            })
        }
        QueryResult::RangeMatrix(series) => json!({
            "resultType": "matrix",
            "result": range_matrix_json(series),
        }),
        QueryResult::Str { ts_ms, value } => json!({
            "resultType": "string",
            "result": [timestamp_seconds(ts_ms), value],
        }),
    }
}
