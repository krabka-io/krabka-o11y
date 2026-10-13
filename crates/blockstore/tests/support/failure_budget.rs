use std::sync::atomic::{AtomicUsize, Ordering};

pub fn consume_failure_budget(remaining_failures: &AtomicUsize) -> bool {
    remaining_failures
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
            (left > 0).then(|| left - 1)
        })
        .is_ok()
}
