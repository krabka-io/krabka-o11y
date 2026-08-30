use super::{
    AtomicOrdering, BufferedLogHotTail, CancellationToken, DeferredWalConsumerConnect, JoinHandle,
    KafkaLogWalConsumer, ServiceReadiness, SharedCompactionFrontier, Time, TimeExt,
    poll_log_hot_tail_once_with_frontier, sleep,
};

/// Spawns a background task that retries `KafkaLogWalConsumer::connect` until
/// it succeeds, then runs the hot-tail poll loop.
///
/// On a cold boot the Kafka broker may not be ready yet. The retry here lets
/// the querier serve its HTTP port immediately (FIX B2).
///
/// A cancel of `token` makes the poll loop exit and calls `consumer.close()`,
/// which sends `LeaveGroup`. That removes the consumer from the broker's group
/// immediately on graceful shutdown.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_wal_hot_tail_connect_and_poll(
    deferred: DeferredWalConsumerConnect,
    hot_tail: BufferedLogHotTail,
    frontier: Option<SharedCompactionFrontier>,
    token: CancellationToken,
    poll_interval: Time,
    reconnect_interval: Time,
    readiness: ServiceReadiness,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut consumer = loop {
            tokio::select! {
                () = token.cancelled() => return,
                result = KafkaLogWalConsumer::connect_with_client_resource_policy(
                    &deferred.bootstrap,
                    deferred.group_id.clone(),
                    deferred.topic.clone(),
                    deferred.client_resource_policy,
                ) => {
                    match result {
                        Ok(c) => break c,
                        Err(error) => {
                            tracing::warn!(%error, "querier WAL consumer connect failed; retrying");
                            tokio::select! {
                                () = token.cancelled() => return,
                                () = sleep(reconnect_interval.to_std()) => {}
                            }
                        }
                    }
                }
            }
        };
        readiness.wal_connected.store(true, AtomicOrdering::SeqCst);
        loop {
            let result = tokio::select! {
                () = token.cancelled() => break,
                result = poll_log_hot_tail_once_with_frontier(&mut consumer, &hot_tail, poll_interval, frontier.as_ref()) => result,
            };
            let should_back_off = match result {
                Ok(decoded) => {
                    readiness.wal_connected.store(true, AtomicOrdering::SeqCst);
                    decoded == 0
                }
                Err(error) => {
                    readiness.wal_connected.store(false, AtomicOrdering::SeqCst);
                    tracing::warn!(%error, "querier WAL hot-tail poll failed; retrying");
                    true
                }
            };
            if should_back_off {
                tokio::select! {
                    () = token.cancelled() => break,
                    () = sleep(poll_interval.to_std()) => {}
                }
            }
        }
        consumer.close().await;
    })
}
