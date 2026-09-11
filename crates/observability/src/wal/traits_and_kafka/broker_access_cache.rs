use super::{
    AclSet, Arc, AtomicBool, AtomicOrdering, BTreeMap, BrokerAccessPolicy, BrokerAccessSource,
    CachedLookup, CachedLookupDecision, CancellationToken, Instant, Mutex, TenantId, TenantLru,
    TimeExt,
};

/// The broker's WAL topic ACLs and each tenant's client quota, held in memory
/// for the authorization and ingest-limit checks.
///
/// A check reads a snapshot and does not wait on the broker. The ACL snapshot
/// is refreshed by [`BrokerAccessCache::keep_fresh`], a task the role
/// supervises, once per TTL. A quota snapshot is refreshed by the first check
/// for that tenant after its TTL, so the broker gets at most one quota lookup
/// per tenant per TTL. The quota cache holds at most
/// [`BrokerAccessPolicy::tenant_capacity`] tenants.
///
/// A snapshot is served until it is `max_staleness` old. While a refresh
/// fails, the last good snapshot is served for that long and `connected`
/// reads `false`, so a readiness gate on that flag reads unmet. A snapshot older than `max_staleness`, or no snapshot at all, fails
/// the check closed. A failed lookup is not retried for one TTL, so a broker
/// that is down does not become a queue of waiting requests.
pub(crate) struct BrokerAccessCache {
    pub(crate) source: Arc<dyn BrokerAccessSource>,
    pub(crate) wal_topic: String,
    pub(crate) policy: BrokerAccessPolicy,
    pub(crate) connected: Arc<AtomicBool>,
    pub(crate) acls: Mutex<CachedLookup<AclSet>>,
    pub(crate) acl_refresh: tokio::sync::Mutex<()>,
    pub(crate) quotas: Mutex<TenantLru<CachedLookup<BTreeMap<String, f64>>>>,
    pub(crate) quota_refresh: tokio::sync::Mutex<()>,
    /// Where the cache reads the time. A test sets a clock it moves by hand.
    pub(crate) clock: Arc<dyn Fn() -> Instant + Send + Sync>,
}

impl BrokerAccessCache {
    pub(crate) fn new(
        source: Arc<dyn BrokerAccessSource>,
        wal_topic: String,
        policy: BrokerAccessPolicy,
        connected: Arc<AtomicBool>,
    ) -> Self {
        Self {
            source,
            wal_topic,
            policy,
            connected,
            acls: Mutex::new(CachedLookup::default()),
            acl_refresh: tokio::sync::Mutex::new(()),
            quotas: Mutex::new(TenantLru::new(policy.tenant_capacity)),
            quota_refresh: tokio::sync::Mutex::new(()),
            clock: Arc::new(Instant::now),
        }
    }

    /// The WAL topic's ACLs, from the snapshot when it may be served.
    ///
    /// # Errors
    ///
    /// Returns the broker's failure reason when no snapshot within the
    /// staleness bound exists and the broker does not answer.
    pub(crate) async fn acls(&self) -> Result<Arc<AclSet>, String> {
        if let Some(served) = self.decided_acls() {
            return served;
        }
        let _refreshing = self.acl_refresh.lock().await;
        // Another request may have refreshed the snapshot during the wait.
        if let Some(served) = self.decided_acls() {
            return served;
        }
        self.fetch_acls().await
    }

    /// Asks the broker for the WAL topic's ACLs and stores the answer.
    ///
    /// # Errors
    ///
    /// Returns the broker's failure reason when the broker does not answer and
    /// no snapshot within the staleness bound exists.
    pub(crate) async fn refresh_acls(&self) -> Result<Arc<AclSet>, String> {
        let _refreshing = self.acl_refresh.lock().await;
        self.fetch_acls().await
    }

    /// Refreshes the ACL snapshot once per TTL until `token` is cancelled.
    pub(crate) async fn keep_fresh(&self, token: CancellationToken) {
        let mut ticks = tokio::time::interval(self.policy.ttl.to_std());
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = token.cancelled() => return,
                _ = ticks.tick() => {
                    if let Err(reason) = self.refresh_acls().await {
                        tracing::warn!(%reason, wal_topic = %self.wal_topic, "broker ACL refresh failed");
                    }
                }
            }
        }
    }

    /// The tenant's client quota, from its snapshot when it may be served.
    ///
    /// # Errors
    ///
    /// Returns the broker's failure reason when the tenant has no quota
    /// snapshot within the staleness bound and the broker does not answer.
    pub(crate) async fn quotas(
        &self,
        tenant: &TenantId,
    ) -> Result<Arc<BTreeMap<String, f64>>, String> {
        if let Some(served) = self.decided_quotas(tenant) {
            return served;
        }
        let _refreshing = self.quota_refresh.lock().await;
        if let Some(served) = self.decided_quotas(tenant) {
            return served;
        }
        let result = self.source.user_quotas(tenant).await;
        let now = (self.clock)();
        self.connected.store(result.is_ok(), AtomicOrdering::SeqCst);
        let mut quotas = self
            .quotas
            .lock()
            .expect("broker quota cache lock poisoned");
        let entry = quotas.get_or_insert_with(tenant, CachedLookup::default);
        match result {
            Ok(quota) => Ok(entry.store(quota, now)),
            Err(reason) => entry.fail(reason, now, &self.policy),
        }
    }

    fn decided_acls(&self) -> Option<Result<Arc<AclSet>, String>> {
        let acls = self.acls.lock().expect("broker ACL cache lock poisoned");
        served(acls.decide((self.clock)(), &self.policy))
    }

    fn decided_quotas(
        &self,
        tenant: &TenantId,
    ) -> Option<Result<Arc<BTreeMap<String, f64>>, String>> {
        let mut quotas = self
            .quotas
            .lock()
            .expect("broker quota cache lock poisoned");
        let entry = quotas.get_mut(tenant)?;
        served(entry.decide((self.clock)(), &self.policy))
    }

    async fn fetch_acls(&self) -> Result<Arc<AclSet>, String> {
        let result = self.source.wal_topic_acls(&self.wal_topic).await;
        let now = (self.clock)();
        self.connected.store(result.is_ok(), AtomicOrdering::SeqCst);
        let mut acls = self.acls.lock().expect("broker ACL cache lock poisoned");
        match result {
            Ok(value) => Ok(acls.store(value, now)),
            Err(reason) => acls.fail(reason, now, &self.policy),
        }
    }
}

fn served<T>(decision: CachedLookupDecision<T>) -> Option<Result<Arc<T>, String>> {
    match decision {
        CachedLookupDecision::Serve(value) => Some(Ok(value)),
        CachedLookupDecision::Unavailable(reason) => Some(Err(reason)),
        CachedLookupDecision::Refresh => None,
    }
}
