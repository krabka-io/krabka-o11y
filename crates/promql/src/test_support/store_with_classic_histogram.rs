use super::*;

pub(crate) fn store_with_classic_histogram() -> InMemoryMetricStore {
    store_with_series_multi(&[
        TestSeriesValue::builder()
            .selector("http_request_duration_seconds_bucket{le=\"0.1\"}")
            .value(0.0)
            .build(),
        TestSeriesValue::builder()
            .selector("http_request_duration_seconds_bucket{le=\"0.2\"}")
            .value(1.0)
            .build(),
        TestSeriesValue::builder()
            .selector("http_request_duration_seconds_bucket{le=\"0.4\"}")
            .value(3.0)
            .build(),
        TestSeriesValue::builder()
            .selector("http_request_duration_seconds_bucket{le=\"+Inf\"}")
            .value(3.0)
            .build(),
    ])
}
