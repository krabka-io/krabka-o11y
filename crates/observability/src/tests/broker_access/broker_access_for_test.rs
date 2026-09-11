use super::*;

pub(crate) const WAL_TOPIC: &str = "__krabka_observability_logs_wal";

pub(crate) fn tenant_for_test(name: &str) -> TenantId {
    TenantId::new(name).expect("a valid tenant id")
}

/// A ten-second TTL, a one-minute staleness bound, and room for `capacity`
/// tenants.
pub(crate) fn policy_for_test(capacity: usize) -> BrokerAccessPolicy {
    BrokerAccessPolicy {
        ttl: secs(10),
        max_staleness: secs(60),
        tenant_capacity: NonZeroUsize::new(capacity).expect("a nonzero capacity"),
    }
}

/// A clock that moves only when the test moves it.
pub(crate) struct ManualClock {
    pub(crate) base: Instant,
    pub(crate) offset: Mutex<Duration>,
}

impl ManualClock {
    pub(crate) fn advance(&self, by: Time) {
        *self.offset.lock().expect("clock lock") += by.to_std();
    }

    pub(crate) fn now(&self) -> Instant {
        self.base + *self.offset.lock().expect("clock lock")
    }
}

/// A cache over `source` under [`policy_for_test`], the flag it reports the
/// broker's state on, and the clock it reads.
pub(crate) fn broker_access_for_test(
    source: &Arc<CountingBrokerAccess>,
    capacity: usize,
) -> (BrokerAccessCache, Arc<AtomicBool>, Arc<ManualClock>) {
    let connected = Arc::new(AtomicBool::new(false));
    let clock = Arc::new(ManualClock {
        base: Instant::now(),
        offset: Mutex::new(Duration::ZERO),
    });
    let mut cache = BrokerAccessCache::new(
        Arc::clone(source) as Arc<dyn BrokerAccessSource>,
        WAL_TOPIC.to_string(),
        policy_for_test(capacity),
        Arc::clone(&connected),
    );
    let reading = Arc::clone(&clock);
    cache.clock = Arc::new(move || reading.now());
    (cache, connected, clock)
}
