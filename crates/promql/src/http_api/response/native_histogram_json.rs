use super::{NativeHistogram, Value, json, native_histogram_buckets_json, sample_string};

pub(crate) fn native_histogram_json(histogram: &NativeHistogram) -> Value {
    json!({
        "count": sample_string(histogram.count),
        "sum": sample_string(histogram.sum),
        "buckets": native_histogram_buckets_json(histogram),
    })
}
