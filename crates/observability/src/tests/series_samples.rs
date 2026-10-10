//! JSON range and instant series for the vector binary-operation tests.

use super::prelude::{Value, json};

/// One sample of a JSON series: its timestamp and its value as text.
#[derive(Clone, Copy)]
pub(crate) struct TimedSample<'a> {
    pub(crate) timestamp: i64,
    pub(crate) sample_value: &'a str,
}

pub(crate) const fn timed_sample(timestamp: i64, sample_value: &str) -> TimedSample<'_> {
    TimedSample {
        timestamp,
        sample_value,
    }
}

/// A range series labelled `app="api"` holding `samples` as
/// `[timestamp, value]`.
pub(crate) fn range_series(samples: &[TimedSample<'_>]) -> Value {
    json!({
        "metric": {"app": "api"},
        "values": samples
            .iter()
            .map(|sample| json!([sample.timestamp, sample.sample_value]))
            .collect::<Vec<_>>(),
    })
}

/// An unlabelled instant series holding one sample.
pub(crate) fn instant_series(sample: TimedSample<'_>) -> Value {
    json!({"metric": {}, "value": [sample.timestamp, sample.sample_value]})
}

/// The `(timestamp, value)` pairs of a range series built by
/// [`range_series`].
pub(crate) fn range_pairs(series: &Value) -> Vec<(i64, String)> {
    series
        .get("values")
        .and_then(Value::as_array)
        .expect("the series has values")
        .iter()
        .map(|sample| {
            (
                sample[0].as_i64().expect("a timestamp"),
                sample[1].as_str().expect("a value").to_string(),
            )
        })
        .collect()
}
