use super::*;

pub(crate) fn store_with_classic_histogram() -> InMemoryMetricStore {
    store_with_series_multi(&[
        ("http_request_duration_seconds_bucket{le=\"0.1\"}", 0.0),
        ("http_request_duration_seconds_bucket{le=\"0.2\"}", 1.0),
        ("http_request_duration_seconds_bucket{le=\"0.4\"}", 3.0),
        ("http_request_duration_seconds_bucket{le=\"+Inf\"}", 3.0),
    ])
}
