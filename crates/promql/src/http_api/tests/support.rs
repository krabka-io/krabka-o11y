//! Shared test helpers for this test area.
use super::*;

pub(crate) mod annotation_query_uri {
use url::form_urlencoded;

// Builds a GET URI for `route` with `query` and a fixed single-step evaluation
// window, so an instant query and a range query cover the same instant.
pub(crate) fn annotation_query_uri(route: &str, query: &str) -> String {
    let form = form_urlencoded::Serializer::new(String::new())
        .append_pair("query", query)
        .append_pair("time", "0")
        .append_pair("start", "0")
        .append_pair("end", "0")
        .append_pair("step", "60")
        .finish();
    format!("{route}?{form}")
}
}

pub(crate) mod annotation_store {
use super::*;

// One float series `up` and one native-histogram series `h`, both at timestamp
// zero. A float-only query warns, a histogram-against-float comparison raises an
// info, and a selector that matches nothing raises neither.
pub(crate) fn annotation_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    let mut up = Labels::new();
    up.insert("__name__", "up");
    up.insert("job", "api");
    store.push_float("tenant-a", up, 0, 1.0);

    let mut histogram = Labels::new();
    histogram.insert("__name__", "h");
    histogram.insert("job", "api");
    store.push_histogram(
        "tenant-a",
        histogram,
        0,
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 4.0,
            sum: 5.0,
            positive_spans: vec![],
            positive_counts: vec![],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: None,
            start_timestamp_ms: None,
        },
    );
    store
}
}

pub(crate) mod first_vector_sample {
//! The first sample of an instant query's vector response, as the HTTP API's
//! own unit tests and the router tests of //crates/metrics-service check it.
//!
//! //crates/metrics-service reaches this file with `#[path]`, so it names
//! nothing from this crate.

/// The `job` label and the formatted value that the first sample of a
/// vector response carries.
pub struct ExpectedVectorSample<'a> {
    pub job: &'a str,
    pub value: &'a str,
}

/// Checks that the first sample of the vector in `body` carries `expected`.
pub fn check_first_vector_sample(body: &serde_json::Value, expected: &ExpectedVectorSample<'_>) {
    assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some(expected.job));
    assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some(expected.value));
}
}

pub(crate) mod two_series_store {
use super::*;

// Two `up` series in one tenant, at timestamp zero. Every series-cap test needs
// a tenant whose selected-series count is above a cap of one.
pub(crate) fn two_series_store() -> InMemoryMetricStore {
    let mut store = InMemoryMetricStore::new();
    for job in ["api", "worker"] {
        let mut labels = Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", job);
        store.push_float("tenant-a", labels, 0, 1.0);
    }
    store
}
}

