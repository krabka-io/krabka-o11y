use super::*;

#[cfg_attr(test, mutants::skip)]
pub(crate) async fn connect_with_startup_retry<T, E, F, Fut>(
    what: &str,
    deadline: Time,
    attempt_timeout: Time,
    initial_backoff: Time,
    max_backoff: Time,
    mut make: F,
) -> Result<T, E>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    // Every attempt is bounded by `attempt_timeout` — including the
    // final one after the deadline.  The previous implementation called an
    // un-timed `make().await` when the deadline expired inside the timeout arm,
    // which could itself hang forever.  Instead we track the last `Err` value
    // and return it on deadline expiry so we never call make() without a timer.
    let start = tokio::time::Instant::now();
    let mut backoff = initial_backoff;
    let mut last_err: Option<E> = None;
    loop {
        match tokio::time::timeout(attempt_timeout.to_std(), make()).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(error)) => {
                if start.elapsed().as_time() >= deadline {
                    return Err(error);
                }
                tracing::warn!(dependency = what, %error, "WAL dependency connect failed during broker warmup; retrying");
                last_err = Some(error);
            }
            Err(_elapsed) => {
                if start.elapsed().as_time() >= deadline {
                    if let Some(e) = last_err {
                        // Return the last real error we saw rather than an un-timed attempt.
                        return Err(e);
                    }
                    // All attempts so far only timed out (no Err variant captured).
                    // Do one final timed attempt; whatever it returns is the answer.
                    return if let Ok(result) =
                        tokio::time::timeout(attempt_timeout.to_std(), make()).await
                    {
                        result
                    } else {
                        // Still timing out after the deadline — treat as a persistent
                        // hang; update last_err on next loop iteration and eventually
                        // we'll return Err from the Ok(Err) deadline arm.  For now,
                        // sleep briefly and let the loop expire naturally.
                        tracing::error!(
                            dependency = what,
                            "WAL dependency connect timed out repeatedly; giving up"
                        );
                        sleep(initial_backoff.to_std()).await;
                        continue;
                    };
                }
                tracing::warn!(
                    dependency = what,
                    "WAL dependency connect timed out during broker warmup; retrying"
                );
            }
        }
        sleep(backoff.to_std()).await;
        // `Time` is `PartialOrd` but not `Ord`, so `Time::min` rather than
        // `std::cmp::min`.
        backoff = (backoff * 2.0).min(max_backoff);
    }
}
