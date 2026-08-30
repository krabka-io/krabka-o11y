use super::*;

pub(crate) fn range_matrix_json(series: Vec<RangeSeries>) -> Vec<Value> {
    series
        .into_iter()
        .map(|series| {
            let mut values = Vec::new();
            let mut histograms = Vec::new();
            for (ts_ms, sample) in series.samples {
                match sample {
                    SampleValue::Float(value) => {
                        values.push(json!([timestamp_seconds(ts_ms), sample_string(value)]));
                    }
                    SampleValue::Histogram(histogram) => {
                        histograms.push(json!([
                            timestamp_seconds(ts_ms),
                            native_histogram_json(&histogram)
                        ]));
                    }
                }
            }
            let mut object = Map::new();
            object.insert("metric".to_string(), labels_json(&series.labels));
            if !values.is_empty() {
                object.insert("values".to_string(), Value::Array(values));
            }
            if !histograms.is_empty() {
                object.insert("histograms".to_string(), Value::Array(histograms));
            }
            Value::Object(object)
        })
        .collect()
}
