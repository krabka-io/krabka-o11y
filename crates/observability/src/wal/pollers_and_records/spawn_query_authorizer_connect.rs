use super::{
    Arc, BrokerBackedQueryAuthorizer, CancellationToken, ClientResourcePolicy, JoinHandle,
    LogQueryAuthorizer, ReadinessGate, Time, TimeExt, sleep,
};

/// Spawns a background task that retries `BrokerBackedQueryAuthorizer::connect`
/// until it succeeds, then swaps the unavailable authorizer for the real
/// broker-backed authorizer.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_query_authorizer_connect(
    bootstrap: String,
    topic: String,
    slot: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
    client_resource_policy: ClientResourcePolicy,
    reconnect_interval: Time,
    token: CancellationToken,
    authorization: ReadinessGate,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let authorizer = loop {
            let result = tokio::select! {
                () = token.cancelled() => return,
                result = BrokerBackedQueryAuthorizer::connect(
                &bootstrap,
                topic.clone(),
                client_resource_policy,
                authorization.shared_flag(),
                ) => result,
            };
            match result {
                Ok(a) => break a,
                Err(error) => {
                    tracing::warn!(%error, "querier authorizer connect failed; retrying");
                    tokio::select! {
                        () = token.cancelled() => return,
                        () = sleep(reconnect_interval.to_std()) => {}
                    }
                }
            }
        };
        // Scope the write guard: every query takes a read lock on this slot, so
        // holding the writer across the `token.cancelled()` await below would
        // block every query for the life of the service.
        {
            let mut guard = slot.write().await;
            *guard = Arc::new(authorizer);
        }
        authorization.mark_ready();
        tracing::info!("querier query authorizer connected; broker-backed ACL checks active");
        token.cancelled().await;
    })
}
