use super::*;

/// The refresh task asks the broker at its start and then once per TTL, so a
/// request finds a fresh snapshot without asking itself. A cancel stops it.
///
/// The task sleeps on the runtime's timer, so this test runs on real time with
/// a TTL of 20 milliseconds and checks counts with a margin.
#[tokio::test]
pub(crate) async fn keep_fresh_asks_the_broker_once_per_ttl_until_cancelled() {
    let source = Arc::new(CountingBrokerAccess::answering(Ok(
        AclSet::SecurityDisabled,
    )));
    let connected = Arc::new(AtomicBool::new(false));
    let access = BrokerAccessCache::new(
        Arc::clone(&source) as Arc<dyn BrokerAccessSource>,
        "__krabka_observability_logs_wal".to_string(),
        BrokerAccessPolicy {
            ttl: millis(20),
            max_staleness: secs(60),
            tenant_capacity: NonZeroUsize::new(8).expect("a nonzero capacity"),
        },
        Arc::clone(&connected),
    );
    let limiter = Arc::new(BrokerBackedIngestLimiter::with_access(access, secs(1)));
    let token = CancellationToken::new();
    let task = tokio::spawn({
        let limiter = Arc::clone(&limiter);
        let token = token.clone();
        async move { limiter.keep_fresh(token).await }
    });

    tokio::time::sleep(millis(110).to_std()).await;
    let (refreshes, _) = source.lookups();
    check!((3..=7).contains(&refreshes), "{refreshes} refreshes");
    check!(connected.load(AtomicOrdering::SeqCst));

    token.cancel();
    check!(task.await.is_ok());
    let (stopped_at, _) = source.lookups();
    tokio::time::sleep(millis(60).to_std()).await;
    check!(source.lookups().0 == stopped_at);
}
