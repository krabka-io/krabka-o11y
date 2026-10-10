use super::*;

#[tokio::test]
pub(crate) async fn instant_count_and_present_over_time_include_native_histograms() {
    for (query, expected) in [
        ("count_over_time(request_duration_seconds[2m])", 2.0),
        ("present_over_time(request_duration_seconds[2m])", 1.0),
    ] {
        assert_histogram_series_reduction(query, expected).await;
    }
}
