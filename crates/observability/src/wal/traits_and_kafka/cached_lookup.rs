use super::{Arc, BrokerAccessPolicy, Instant, TimeExt};

/// One broker answer, when it arrived, and the last refresh that failed.
#[derive(Debug)]
pub(crate) struct CachedLookup<T> {
    pub(crate) snapshot: Option<(Arc<T>, Instant)>,
    pub(crate) last_failure: Option<(Instant, String)>,
}

/// What a caller does with a [`CachedLookup`] at one instant.
#[derive(Debug, PartialEq)]
pub(crate) enum CachedLookupDecision<T> {
    /// Use this value without asking the broker.
    Serve(Arc<T>),
    /// Fail closed with this reason, without asking the broker.
    Unavailable(String),
    /// Ask the broker.
    Refresh,
}

impl<T> Default for CachedLookup<T> {
    fn default() -> Self {
        Self {
            snapshot: None,
            last_failure: None,
        }
    }
}

impl<T> CachedLookup<T> {
    /// Decides whether a request at `now` reads the snapshot, fails closed, or
    /// asks the broker.
    ///
    /// A snapshot younger than the TTL is served. After a refresh fails, the
    /// broker is not asked again for one TTL: the snapshot is served while it
    /// is within the staleness bound, and without one the check fails closed.
    /// A request therefore never waits on a broker that just failed.
    pub(crate) fn decide(
        &self,
        now: Instant,
        policy: &BrokerAccessPolicy,
    ) -> CachedLookupDecision<T> {
        let ttl = policy.ttl.to_std();
        if let Some((value, fetched_at)) = &self.snapshot
            && now.saturating_duration_since(*fetched_at) < ttl
        {
            return CachedLookupDecision::Serve(Arc::clone(value));
        }
        match &self.last_failure {
            Some((failed_at, reason)) if now.saturating_duration_since(*failed_at) < ttl => {
                self.within_staleness(now, policy).map_or_else(
                    || CachedLookupDecision::Unavailable(reason.clone()),
                    CachedLookupDecision::Serve,
                )
            }
            _ => CachedLookupDecision::Refresh,
        }
    }

    /// The snapshot, while it is no older than the staleness bound.
    pub(crate) fn within_staleness(
        &self,
        now: Instant,
        policy: &BrokerAccessPolicy,
    ) -> Option<Arc<T>> {
        self.snapshot
            .as_ref()
            .filter(|(_, fetched_at)| {
                now.saturating_duration_since(*fetched_at) <= policy.max_staleness.to_std()
            })
            .map(|(value, _)| Arc::clone(value))
    }

    /// Stores a broker answer and forgets the last failure.
    pub(crate) fn store(&mut self, value: T, now: Instant) -> Arc<T> {
        let value = Arc::new(value);
        self.snapshot = Some((Arc::clone(&value), now));
        self.last_failure = None;
        value
    }

    /// Records a failed refresh, and gives the snapshot that may still be
    /// served or the reason to fail closed.
    pub(crate) fn fail(
        &mut self,
        reason: String,
        now: Instant,
        policy: &BrokerAccessPolicy,
    ) -> Result<Arc<T>, String> {
        let served = self.within_staleness(now, policy);
        self.last_failure = Some((now, reason.clone()));
        served.ok_or(reason)
    }
}
