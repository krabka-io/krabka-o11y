use super::*;

/// `connect_with_startup_retry` retries on failure and returns Ok when a later retry succeeds.
#[tokio::test]
pub(crate) async fn connect_with_startup_retry_retries_then_succeeds() {
    use std::sync::atomic::{AtomicU32, Ordering as AO};
    let counter = Arc::new(AtomicU32::new(0));
    let counter2 = counter.clone();

    let result: Result<u32, String> = connect_with_startup_retry(
        "test",
        secs(10),
        secs(1),
        millis(1),
        millis(10),
        move || {
            let c = counter2.clone();
            async move {
                let n = c.fetch_add(1, AO::SeqCst);
                if n < 2 {
                    Err(format!("not ready yet (attempt {n})"))
                } else {
                    Ok(99u32)
                }
            }
        },
    )
    .await;

    assert_eq!(result.unwrap(), 99);
    assert!(counter.load(std::sync::atomic::Ordering::SeqCst) >= 3);
}
