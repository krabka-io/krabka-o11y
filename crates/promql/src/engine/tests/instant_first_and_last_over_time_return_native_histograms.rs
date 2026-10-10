use super::*;

#[tokio::test]
pub(crate) async fn instant_first_and_last_over_time_return_native_histograms() {
    for (query, expected) in [
        (
            "histogram_sum(first_over_time(request_duration_seconds[2m]))",
            10.0,
        ),
        (
            "histogram_sum(last_over_time(request_duration_seconds[2m]))",
            20.0,
        ),
    ] {
        assert_histogram_series_reduction(query, expected).await;
    }
}
