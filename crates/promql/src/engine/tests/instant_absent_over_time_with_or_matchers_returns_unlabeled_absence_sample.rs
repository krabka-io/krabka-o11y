use super::*;

#[tokio::test]
pub(crate) async fn instant_absent_over_time_with_or_matchers_returns_unlabeled_absence_sample() {
    assert_lone_unlabeled_value_at(
        &FloatStore::default().engine(),
        LoneValueQuery {
            query: r#"absent_over_time(up{job="api" or job="web"}[1m])"#,
            time_ms: 120_000,
            want: 1.0,
        },
    )
    .await;
}
