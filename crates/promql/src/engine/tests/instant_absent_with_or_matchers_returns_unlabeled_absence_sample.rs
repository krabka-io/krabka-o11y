use super::*;

#[tokio::test]
pub(crate) async fn instant_absent_with_or_matchers_returns_unlabeled_absence_sample() {
    assert_lone_unlabeled_value_at(
        &FloatStore::default().engine(),
        LoneValueQuery {
            query: r#"absent(up{job="api" or job="web"})"#,
            time_ms: 10_000,
            want: 1.0,
        },
    )
    .await;
}
