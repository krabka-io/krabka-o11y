// Waiting on the requests a real client sends to a capturing listener.

use std::{collections::BTreeSet, time::Duration};

use tokio::{sync::Mutex, time::error::Elapsed};

// Waits up to 45 seconds for `seen` to hold exactly `expected`, then checks
// that it does.
pub async fn expect_seen<T: Ord + std::fmt::Debug>(
    seen: &Mutex<BTreeSet<T>>,
    expected: &BTreeSet<T>,
) -> Result<(), Elapsed> {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if *seen.lock().await == *expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    assert2::assert!(*seen.lock().await == *expected);
    Ok(())
}
