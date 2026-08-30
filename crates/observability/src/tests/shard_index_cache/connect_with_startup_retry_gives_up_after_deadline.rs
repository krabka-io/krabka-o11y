use super::*;

/// `connect_with_startup_retry` returns the error after the deadline is exceeded.
#[tokio::test]
pub(crate) async fn connect_with_startup_retry_gives_up_after_deadline() {
    let result: Result<u32, String> = connect_with_startup_retry(
        "test",
        millis(50), // very short deadline
        millis(10),
        millis(1),
        millis(10),
        || async { Err::<u32, String>("always fails".to_string()) },
    )
    .await;

    assert!(result.is_err(), "expected Err after deadline");
    assert_eq!(result.unwrap_err(), "always fails");
}
