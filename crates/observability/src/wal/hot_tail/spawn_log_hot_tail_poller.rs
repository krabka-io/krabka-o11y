use super::*;

#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_log_hot_tail_poller(
    consumer: Arc<tokio::sync::Mutex<Box<dyn LogWalConsumer>>>,
    hot_tail: BufferedLogHotTail,
    frontier: Option<SharedCompactionFrontier>,
    poll_interval: Time,
    token: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let result = tokio::select! {
                () = token.cancelled() => return,
                result = async {
                let mut consumer = consumer.lock().await;
                poll_log_hot_tail_once_with_frontier(
                    consumer.as_mut(),
                    &hot_tail,
                    poll_interval,
                    frontier.as_ref(),
                )
                .await
                } => result,
            };
            let should_back_off = match result {
                Ok(decoded) => decoded == 0,
                Err(error) => {
                    tracing::warn!(%error, "querier WAL hot-tail poll failed; retrying");
                    true
                }
            };
            if should_back_off {
                tokio::select! {
                    () = token.cancelled() => return,
                    () = sleep(poll_interval.to_std()) => {}
                }
            }
        }
    })
}
