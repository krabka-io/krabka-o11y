use super::*;

#[tokio::test]
pub(crate) async fn instant_count_and_present_over_time_include_native_histograms() {
    for case in [
        QueryValueCase {
            query: "count_over_time(request_duration_seconds[2m])",
            value: 2.0,
        },
        QueryValueCase {
            query: "present_over_time(request_duration_seconds[2m])",
            value: 1.0,
        },
    ] {
        assert_histogram_series_reduction(case.query, case.value).await;
    }
}
