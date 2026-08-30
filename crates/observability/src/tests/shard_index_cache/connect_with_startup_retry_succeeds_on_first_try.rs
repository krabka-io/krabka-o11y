use super::*;

/// `connect_with_startup_retry` returns Ok immediately when the closure succeeds on the first try.
#[tokio::test]
pub(crate) async fn connect_with_startup_retry_succeeds_on_first_try() {
    let result: Result<u32, String> =
        connect_with_startup_retry("test", secs(5), secs(1), millis(1), millis(10), || async {
            Ok::<u32, String>(42)
        })
        .await;

    assert_eq!(result.unwrap(), 42);
}
