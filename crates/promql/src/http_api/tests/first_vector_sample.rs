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
