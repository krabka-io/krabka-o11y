use super::*;

#[tokio::test]
pub(crate) async fn instant_first_and_last_over_time_return_native_histograms() {
    for case in [
        QueryValueCase {
            query: "histogram_sum(first_over_time(request_duration_seconds[2m]))",
            value: 10.0,
        },
        QueryValueCase {
            query: "histogram_sum(last_over_time(request_duration_seconds[2m]))",
            value: 20.0,
        },
    ] {
        assert_histogram_series_reduction(case.query, case.value).await;
    }
}
