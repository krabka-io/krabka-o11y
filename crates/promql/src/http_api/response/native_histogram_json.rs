use super::{Map, NativeHistogram, Value, json, native_histogram_buckets_json, sample_string};

pub(crate) fn native_histogram_json(histogram: &NativeHistogram) -> Value {
    let mut object = Map::new();
    object.insert("count".to_string(), json!(sample_string(histogram.count)));
    object.insert("sum".to_string(), json!(sample_string(histogram.sum)));
    let buckets = native_histogram_buckets_json(histogram);
    if !buckets.is_empty() {
        object.insert("buckets".to_string(), json!(buckets));
    }
    Value::Object(object)
}
