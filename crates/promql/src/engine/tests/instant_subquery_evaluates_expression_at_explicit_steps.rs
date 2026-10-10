use super::*;

#[tokio::test]
pub(crate) async fn instant_subquery_evaluates_expression_at_explicit_steps() {
    let engine = queue_depth_engine();
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "(queue_depth * 2)[2m:1m]", 120_000)
        .await
        .unwrap();

    let series = lone_matrix_series(&result);
    check!(series.labels.get("__name__").is_none());
    check!(series.labels.get("job") == Some("api"));
    check_series_points(
        series,
        &[
            ExpectedPoint {
                ts_ms: 0,
                value: 2.0,
            },
            ExpectedPoint {
                ts_ms: 60_000,
                value: 4.0,
            },
            ExpectedPoint {
                ts_ms: 120_000,
                value: 6.0,
            },
        ],
    );
}
