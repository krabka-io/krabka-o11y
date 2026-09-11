use super::{Error, Future, ObjectStoreRetryPolicy, sleep, transient_object_store_error, warn};

/// Runs `attempt` until it succeeds, fails permanently, or runs out of budget.
///
/// `attempt` must be re-runnable and must land in the same place every time.
/// Every block key in this repository is derived from the data being written
/// -- tenant, partition, WAL offset range, window bounds -- and none of the
/// writes are conditional, so a repeated attempt overwrites the object the
/// previous one half-wrote rather than adding a second block beside it. An
/// operation that allocated a fresh name per attempt would need a different
/// mechanism, and must not use this one.
///
/// # Errors
/// Returns the last error `attempt` produced: the first permanent one, or the
/// transient one that exhausted `policy.max_attempts`.
pub async fn retry_object_store<T, E, Op, Fut>(
    policy: ObjectStoreRetryPolicy,
    operation: &str,
    mut attempt: Op,
) -> Result<T, E>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: Error + 'static,
{
    let mut backoff = policy.initial_backoff;
    let mut attempts_left = policy.max_attempts.get();
    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        attempts_left -= 1;
        if transient_object_store_error(&error).is_none() {
            return Err(error);
        }
        if attempts_left == 0 {
            warn!(
                operation,
                attempts = policy.max_attempts.get(),
                %error,
                "object-store retry budget exhausted; failing the operation"
            );
            return Err(error);
        }
        warn!(
            operation,
            attempts_left,
            backoff_ms = u64::try_from(backoff.as_millis()).unwrap_or(u64::MAX),
            %error,
            "transient object-store failure; retrying"
        );
        sleep(backoff).await;
        backoff = backoff.saturating_mul(2).min(policy.max_backoff);
    }
}
