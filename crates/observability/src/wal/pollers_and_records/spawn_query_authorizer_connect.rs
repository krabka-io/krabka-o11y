use super::{
    Arc, BrokerBackedQueryAuthorizer, CancellationToken, DeferredQueryAuthorizerConnect,
    JoinHandle, LogQueryAuthorizer, ReadinessGate, Time, TimeExt, sleep,
};

/// Spawns a background task that retries `BrokerBackedQueryAuthorizer::connect`
/// until it succeeds, swaps the unavailable authorizer for the real one, and
/// then keeps the real one's ACL snapshot fresh until `token` is cancelled.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn spawn_query_authorizer_connect(
    connect: DeferredQueryAuthorizerConnect,
    slot: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
    reconnect_interval: Time,
    token: CancellationToken,
    authorization: ReadinessGate,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let authorizer = loop {
            let result = tokio::select! {
                () = token.cancelled() => return,
                result = BrokerBackedQueryAuthorizer::connect(
                    &connect.bootstrap,
                    connect.topic.clone(),
                    connect.client_resource_policy,
                    connect.security.as_ref(),
                    authorization.shared_flag(),
                    connect.access_policy,
                ) => result,
            };
            match result {
                Ok(a) => break Arc::new(a),
                Err(error) => {
                    tracing::warn!(%error, "query authorizer connect failed; retrying");
                    tokio::select! {
                        () = token.cancelled() => return,
                        () = sleep(reconnect_interval.to_std()) => {}
                    }
                }
            }
        };
        // Scope the write guard: every query takes a read lock on this slot, so
        // holding the writer across the refresh loop below would block every
        // query for the life of the service.
        {
            let mut guard = slot.write().await;
            *guard = Arc::clone(&authorizer) as Arc<dyn LogQueryAuthorizer>;
        }
        authorization.mark_ready();
        tracing::info!("query authorizer connected; broker-backed ACL checks active");
        authorizer.keep_fresh(token).await;
    })
}
