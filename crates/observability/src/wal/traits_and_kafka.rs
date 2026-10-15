use krabka_client_core::{ClientError, ClientSecurity};
use krabka_units::convert::{ByteRateExt, StdDurationExt, TimeExt};

use crate::{
    AclEntry, AclEntryFilter, AclOperation, AdminClient, AdminError, Arc, AtomicBool,
    AtomicOrdering, BTreeMap, ByteRate, ByteSize, ByteSizeExt, CancellationToken,
    ClientResourcePolicy, IngestLimitError, Instant, KafkaWalRecord, Mutex, NonZeroUsize,
    PatternType, PermissionType, QueryAuthorizationError, ReadinessGate, ResourceType,
    ServiceConfig, ServiceConfigError, TenantId, Time, WalConsumerError, WalLogRecord, WalPosition,
    WalSinkError, async_trait, measured_size,
    server_security::Principal,
    wal_client_security::with_client_security,
    wal_produce::{WalBatchError, write_batch_serially},
};
pub(crate) const PRODUCER_BYTE_RATE_QUOTA_KEY: &str = "producer_byte_rate";

pub(crate) fn acl_matches_tenant_wal_read(
    acl: &AclEntry,
    principal: &str,
    wal_topic: &str,
) -> bool {
    acl.resource_type == ResourceType::Topic
        && matches!(acl.operation, AclOperation::All | AclOperation::Read)
        && (acl.principal == principal || acl.principal == "User:*")
        && matches_acl_topic_pattern(acl, wal_topic)
}

pub(crate) fn acl_matches_tenant_wal_write(
    acl: &AclEntry,
    principal: &str,
    wal_topic: &str,
) -> bool {
    acl.resource_type == ResourceType::Topic
        && matches!(acl.operation, AclOperation::All | AclOperation::Write)
        && (acl.principal == principal || acl.principal == "User:*")
        && matches_acl_topic_pattern(acl, wal_topic)
}

pub(crate) fn matches_acl_topic_pattern(acl: &AclEntry, wal_topic: &str) -> bool {
    match acl.pattern_type {
        PatternType::Literal => acl.resource_name == wal_topic || acl.resource_name == "*",
        PatternType::Match => false,
        PatternType::Prefixed => wal_topic.starts_with(&acl.resource_name),
    }
}

/// Classifies one `DescribeAcls` answer.
///
/// Only Kafka error 54, `SECURITY_DISABLED`, means that the broker has no
/// authorizer. Every other error stays an error, so a check that cannot reach
/// the ACLs fails closed.
pub(crate) fn acl_set_from_describe(
    result: Result<Vec<AclEntry>, AdminError>,
) -> Result<AclSet, AdminError> {
    match result {
        Ok(entries) => Ok(AclSet::Configured(entries)),
        Err(AdminError::Broker {
            api: "DescribeAcls",
            code: 54,
            ..
        }) => Ok(AclSet::SecurityDisabled),
        Err(error) => Err(error),
    }
}

/// The connection options of the admin client that the query authorizer and
/// the ingest limiter use.
///
/// `security` is the WAL client security that the service loaded. `None`
/// connects in plain text.
pub(crate) fn admin_connection_options(
    client_resource_policy: ClientResourcePolicy,
    security: Option<&ClientSecurity>,
) -> krabka_client_core::ConnectionOptions {
    with_client_security(
        krabka_client_core::ConnectionOptions {
            dispatch_queue_capacity: client_resource_policy.dispatch_queue_capacity,
            frame_max: client_resource_policy.frame_max,
            ..krabka_client_core::ConnectionOptions::default()
        },
        security,
    )
}

/// Maps a record timestamp to its bucket key.
///
/// `bucket_width` is the width of a hot-tail time bucket. It is coarse enough
/// that a wide retention window holds few buckets, and fine enough that a
/// typical query window of minutes to hours only touches the buckets it
/// overlaps. The function uses [`i64::div_euclid`], so negative, pre-epoch,
/// timestamps still bucket monotonically, and the bucket that contains a given
/// timestamp is unambiguous.
pub(crate) fn hot_tail_bucket_key(timestamp_ns: i64, bucket_width: Time) -> i64 {
    timestamp_ns.div_euclid(bucket_width.nanos_i64())
}

pub(crate) fn ingest_quota_bytes(records: &[WalLogRecord]) -> ByteSize {
    measured_size(
        records
            .iter()
            .map(|record| {
                record.tenant.len()
                    + record.line.len()
                    + std::mem::size_of_val(&record.timestamp_ns)
                    + record
                        .labels
                        .iter()
                        .map(|entry| entry.0.len() + entry.1.len())
                        .sum::<usize>()
                    + record
                        .structured_metadata
                        .iter()
                        .map(|entry| entry.0.len() + entry.1.len())
                        .sum::<usize>()
            })
            .sum(),
    )
}

/// The `DescribeAcls` filters whose union holds every ACL that can apply to
/// `wal_topic`.
///
/// `krabka-client-admin` exposes only the `LITERAL` and `PREFIXED` pattern
/// filters, not Kafka's `MATCH`. A literal filter on the topic name returns
/// only the ACLs that name the topic exactly, and misses a literal `*` and
/// every prefixed ACL. So three filters are necessary: the topic by name, the
/// wildcard by name, and every prefixed topic ACL. Each one is narrower than
/// the whole cluster's ACL set.
pub(crate) fn wal_topic_acl_filters(wal_topic: &str) -> [AclEntryFilter; 3] {
    let topic = |pattern_type, resource_name: Option<&str>| AclEntryFilter {
        resource_type: Some(ResourceType::Topic),
        resource_name: resource_name.map(str::to_owned),
        pattern_type: Some(pattern_type),
        ..AclEntryFilter::default()
    };
    [
        topic(PatternType::Literal, Some(wal_topic)),
        topic(PatternType::Literal, Some("*")),
        topic(PatternType::Prefixed, None),
    ]
}

/// What the broker answers when the logs path asks for the WAL topic's ACLs.
///
/// The two variants must not collapse into one list.
/// [`check_tenant_wal_read_acl`](check_tenant_wal_read_acl) and
/// [`check_tenant_wal_write_acl`](check_tenant_wal_write_acl) decide
/// each answer together with the principal of the request:
///
/// | ACLs | Unauthenticated request | Authenticated request |
/// | --- | --- | --- |
/// | `SecurityDisabled` | allow | allow |
/// | `Configured`, empty | allow | deny |
/// | `Configured`, not empty | the matching ACLs decide | the matching ACLs decide |
///
/// An empty list does not show that the broker enforces ACLs. The pinned
/// Krabka broker never answers Kafka error 54. Its default authorizer is
/// `AllowAllAuthorizer`, and that authorizer answers `DescribeAcls` with an
/// empty list. Error 54 is the answer of an Apache Kafka broker with no
/// authorizer. So a stock Krabka broker with no ACLs gives
/// `Configured(vec![])`.
///
/// Without a credentials file, the only ACL principal is `User:{tenant}`,
/// and the request names its tenant itself. A deny on an empty list then
/// refuses every tenant of a stock broker, and it adds no security. With a
/// credentials file, the principal is real, and no ACL grants it anything. So
/// the check denies, as the Kafka authorizer refuses a principal that no ACL
/// names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AclSet {
    /// The broker runs no authorizer. `DescribeAcls` answers Kafka error 54,
    /// `SECURITY_DISABLED`.
    SecurityDisabled,
    /// The broker holds these topic ACLs. An empty list allows only an
    /// unauthenticated request.
    Configured(Vec<AclEntry>),
}

/// A [`BrokerAccessSource`] that asks a broker over one admin connection.
///
/// The connection is behind an async mutex, so the lookups are serial. Only
/// [`BrokerAccessCache`](BrokerAccessCache) calls this, at most once
/// per TTL for the ACLs and once per TTL for each tenant's quota, so no
/// request waits in a queue on this mutex.
pub(crate) struct AdminBrokerAccess {
    pub(crate) admin: tokio::sync::Mutex<AdminClient>,
}

impl AdminBrokerAccess {
    /// Connects the admin client to `bootstrap` under `security`.
    pub(crate) async fn connect(
        bootstrap: &str,
        client_resource_policy: ClientResourcePolicy,
        security: Option<&ClientSecurity>,
    ) -> Result<Arc<Self>, AdminError> {
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            admin_connection_options(client_resource_policy, security),
        )
        .await?;
        Ok(Arc::new(Self {
            admin: tokio::sync::Mutex::new(admin),
        }))
    }
}

#[async_trait]
impl BrokerAccessSource for AdminBrokerAccess {
    #[cfg_attr(test, mutants::skip)]
    async fn wal_topic_acls(&self, wal_topic: &str) -> Result<AclSet, String> {
        let mut admin = self.admin.lock().await;
        let mut entries = Vec::new();
        for filter in wal_topic_acl_filters(wal_topic) {
            match acl_set_from_describe(admin.describe_acls(&filter).await) {
                Ok(AclSet::Configured(found)) => entries.extend(found),
                Ok(AclSet::SecurityDisabled) => return Ok(AclSet::SecurityDisabled),
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(AclSet::Configured(entries))
    }

    #[cfg_attr(test, mutants::skip)]
    async fn user_quotas(&self, tenant: &TenantId) -> Result<BTreeMap<String, f64>, String> {
        let mut admin = self.admin.lock().await;
        match admin.describe_user_quotas(tenant.as_str()).await {
            Ok(quota) => Ok(quota),
            // A broker without client quotas: error 35 is
            // `UNSUPPORTED_VERSION`, and a client that cannot speak API 48 at
            // all never sends the request. Neither has a quota to enforce.
            Err(
                AdminError::Broker {
                    api: "DescribeClientQuotas",
                    code: 35,
                    ..
                }
                | AdminError::Transport(ClientError::IncompatibleVersion { api_key: 48, .. }),
            ) => Ok(BTreeMap::new()),
            Err(error) => Err(error.to_string()),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AllowAllIngestLimiter;

#[async_trait]
impl LogIngestLimiter for AllowAllIngestLimiter {
    async fn check(
        &self,
        _principal: &Principal,
        _tenant: &TenantId,
        _records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AllowAllQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for AllowAllQueryAuthorizer {
    async fn check(
        &self,
        _principal: &Principal,
        _tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        Ok(())
    }
}

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

/// How long [`BrokerAccessCache`](BrokerAccessCache) trusts a broker
/// answer, and how many tenants' quotas it holds.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct BrokerAccessPolicy {
    /// The age at which a snapshot is refreshed.
    pub(crate) ttl: Time,
    /// The age above which a snapshot is not served. A check then fails
    /// closed until the broker answers again.
    pub(crate) max_staleness: Time,
    /// The most tenants whose quota snapshot and rate bucket are held.
    pub(crate) tenant_capacity: NonZeroUsize,
}

impl BrokerAccessPolicy {
    /// The policy that `config` sets.
    ///
    /// # Errors
    ///
    /// Returns [`ServiceConfigError::BrokerAccessStalenessBelowTtl`] when the
    /// staleness bound is shorter than the TTL, because a snapshot would then
    /// stop being served before its first refresh is due.
    pub(crate) fn for_config(config: &ServiceConfig) -> Result<Self, ServiceConfigError> {
        if config.broker_access_max_staleness < config.broker_access_cache_ttl {
            return Err(ServiceConfigError::BrokerAccessStalenessBelowTtl);
        }
        Ok(Self {
            ttl: config.broker_access_cache_ttl,
            max_staleness: config.broker_access_max_staleness,
            tenant_capacity: config.broker_access_tenant_capacity,
        })
    }
}

/// The broker lookups that tenant authorization and ingest quotas read.
///
/// [`AdminBrokerAccess`](AdminBrokerAccess) asks a broker.
/// [`BrokerAccessCache`](BrokerAccessCache) holds the answers, so a
/// request reads memory and not the broker. The seam also lets a test count
/// how often the cache asks.
#[async_trait]
pub(crate) trait BrokerAccessSource: Send + Sync + 'static {
    /// The ACLs that can apply to `wal_topic`, or why the broker did not
    /// answer.
    async fn wal_topic_acls(&self, wal_topic: &str) -> Result<AclSet, String>;

    /// The client quotas the broker holds for `tenant`, or why the broker did
    /// not answer. An empty map means that the tenant has no quota.
    async fn user_quotas(&self, tenant: &TenantId) -> Result<BTreeMap<String, f64>, String>;
}

/// Allows a push when the broker's ACLs grant the request's ACL principal
/// write on the WAL topic, and the push fits the tenant's `producer_byte_rate`
/// quota.
///
/// The ACL principal is `User:{name}` for an authenticated request and
/// `User:{tenant}` otherwise. [`check_tenant_wal_write_acl`] gives the whole
/// rule. The ACLs and the quota come from a [`BrokerAccessCache`]. The rate buckets
/// hold at most the policy's tenant capacity. A tenant whose bucket is removed
/// to make room gets a full bucket on its next push, which allows one burst
/// window of bytes early.
pub(crate) struct BrokerBackedIngestLimiter {
    pub(crate) access: BrokerAccessCache,
    pub(crate) burst_window: Time,
    pub(crate) buckets: Mutex<TenantLru<IngestQuotaBucket>>,
}

impl BrokerBackedIngestLimiter {
    pub(crate) async fn connect(
        bootstrap: &str,
        wal_topic: String,
        client_resource_policy: ClientResourcePolicy,
        security: Option<&ClientSecurity>,
        burst_window: Time,
        policy: BrokerAccessPolicy,
    ) -> Result<Self, AdminError> {
        let source =
            AdminBrokerAccess::connect(bootstrap, client_resource_policy, security).await?;
        Ok(Self::with_access(
            BrokerAccessCache::new(source, wal_topic, policy, Arc::new(AtomicBool::new(false))),
            burst_window,
        ))
    }

    pub(crate) fn with_access(access: BrokerAccessCache, burst_window: Time) -> Self {
        let capacity = access.policy.tenant_capacity;
        Self {
            access,
            burst_window,
            buckets: Mutex::new(TenantLru::new(capacity)),
        }
    }
}

#[async_trait]
impl LogIngestLimiter for BrokerBackedIngestLimiter {
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
        records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        let unavailable = |reason| IngestLimitError::Unavailable {
            tenant: tenant.to_string(),
            reason,
        };
        let acls = self.access.acls().await.map_err(unavailable)?;
        check_tenant_wal_write_acl(principal, tenant, &self.access.wal_topic, &acls)?;
        let quota = self.access.quotas(tenant).await.map_err(unavailable)?;

        let Some(raw_rate) = quota.get(PRODUCER_BYTE_RATE_QUOTA_KEY).copied() else {
            return Ok(());
        };
        if !raw_rate.is_finite() || raw_rate <= 0.0 {
            return Ok(());
        }
        let rate = ByteRate::from_bytes_per_sec_f64(raw_rate);

        let batch = ingest_quota_bytes(records);
        if batch == <ByteSize as ByteSizeExt>::ZERO {
            return Ok(());
        }

        let mut buckets = self.buckets.lock().expect("ingest quota lock poisoned");
        let bucket =
            buckets.get_or_insert_with(tenant, || IngestQuotaBucket::new(rate, self.burst_window));
        bucket.update_rate(rate);
        if bucket.consume(batch) {
            return Ok(());
        }

        let rate = rate.bytes_per_sec_f64();
        let bytes = batch.bytes_usize();
        Err(IngestLimitError::RateLimited {
            tenant: tenant.to_string(),
            reason: format!(
                "{PRODUCER_BYTE_RATE_QUOTA_KEY} quota {rate:.0} bytes/s exceeded by {bytes} byte ingest batch"
            ),
        })
    }

    async fn keep_fresh(&self, token: CancellationToken) {
        self.access.keep_fresh(token).await;
    }
}

/// Allows a read when the broker's ACLs grant the request's ACL principal read
/// on the WAL topic.
///
/// The ACL principal is `User:{name}` for an authenticated request and
/// `User:{tenant}` otherwise. [`check_tenant_wal_read_acl`] gives the whole
/// rule. The ACLs come from a [`BrokerAccessCache`], so a check does not wait
/// on the broker. See that type for how stale an answer may be.
pub(crate) struct BrokerBackedQueryAuthorizer {
    pub(crate) access: BrokerAccessCache,
}

impl BrokerBackedQueryAuthorizer {
    /// Connects the admin client under `security`, and holds its answers
    /// under `policy`.
    ///
    /// `connected` reads `true` while the last ACL lookup succeeded.
    pub(crate) async fn connect(
        bootstrap: &str,
        wal_topic: String,
        client_resource_policy: ClientResourcePolicy,
        security: Option<&ClientSecurity>,
        connected: Arc<AtomicBool>,
        policy: BrokerAccessPolicy,
    ) -> Result<Self, AdminError> {
        let source =
            AdminBrokerAccess::connect(bootstrap, client_resource_policy, security).await?;
        Ok(Self {
            access: BrokerAccessCache::new(source, wal_topic, policy, connected),
        })
    }

    /// Refreshes the ACL snapshot once per TTL until `token` is cancelled.
    pub(crate) async fn keep_fresh(&self, token: CancellationToken) {
        self.access.keep_fresh(token).await;
    }
}

#[async_trait]
impl LogQueryAuthorizer for BrokerBackedQueryAuthorizer {
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        let acls =
            self.access
                .acls()
                .await
                .map_err(|reason| QueryAuthorizationError::Unavailable {
                    tenant: tenant.to_string(),
                    reason,
                })?;
        check_tenant_wal_read_acl(principal, tenant, &self.access.wal_topic, &acls)
    }
}

/// One broker answer, when it arrived, and the last refresh that failed.
#[derive(Debug, PartialEq)]
pub(crate) struct CachedSnapshot<T> {
    pub(crate) value: Arc<T>,
    pub(crate) fetched_at: Instant,
}

#[derive(Debug, PartialEq)]
pub(crate) struct CachedFailure {
    pub(crate) failed_at: Instant,
    pub(crate) reason: String,
}

#[derive(Debug)]
pub(crate) struct CachedLookup<T> {
    pub(crate) snapshot: Option<CachedSnapshot<T>>,
    pub(crate) last_failure: Option<CachedFailure>,
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
        if let Some(snapshot) = &self.snapshot
            && now.saturating_duration_since(snapshot.fetched_at) < ttl
        {
            return CachedLookupDecision::Serve(Arc::clone(&snapshot.value));
        }
        match &self.last_failure {
            Some(failure) if now.saturating_duration_since(failure.failed_at) < ttl => {
                self.within_staleness(now, policy).map_or_else(
                    || CachedLookupDecision::Unavailable(failure.reason.clone()),
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
            .filter(|snapshot| {
                now.saturating_duration_since(snapshot.fetched_at) <= policy.max_staleness.to_std()
            })
            .map(|snapshot| Arc::clone(&snapshot.value))
    }

    /// Stores a broker answer and forgets the last failure.
    pub(crate) fn store(&mut self, value: T, now: Instant) -> Arc<T> {
        let value = Arc::new(value);
        self.snapshot = Some(CachedSnapshot {
            value: Arc::clone(&value),
            fetched_at: now,
        });
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
        self.last_failure = Some(CachedFailure {
            failed_at: now,
            reason: reason.clone(),
        });
        served.ok_or(reason)
    }
}

/// Allows `principal` to read the data of `tenant` from the WAL topic when the
/// ACLs allow it.
///
/// The ACL principal is [`Principal::acl_principal`]: `User:{name}` for an
/// authenticated request, and `User:{tenant}` for an unauthenticated request.
///
/// - [`AclSet::SecurityDisabled`] allows every read.
/// - An empty [`AclSet::Configured`] allows an unauthenticated read, and it
///   refuses an authenticated read. [`AclSet`] gives the reason, with the
///   evidence from the pinned Krabka broker.
/// - A non-empty [`AclSet::Configured`] allows the read only when an `Allow`
///   ACL matches the ACL principal and no `Deny` ACL matches it.
///
/// # Errors
///
/// Returns [`QueryAuthorizationError::Unauthorized`] when the ACLs refuse the
/// read.
pub(crate) fn check_tenant_wal_read_acl(
    principal: &Principal,
    tenant: &TenantId,
    wal_topic: &str,
    acls: &AclSet,
) -> Result<(), QueryAuthorizationError> {
    let refusal = tenant_wal_acl_refusal(&TenantWalAclCheck {
        principal,
        tenant,
        wal_topic,
        acls,
        access: WalTopicAccess::Read,
    });
    refusal.map_or(Ok(()), |reason| {
        Err(QueryAuthorizationError::Unauthorized {
            tenant: tenant.to_string(),
            reason,
        })
    })
}

/// Allows `principal` to write the data of `tenant` to the WAL topic when the
/// ACLs allow it.
///
/// The ACL principal is [`Principal::acl_principal`]: `User:{name}` for an
/// authenticated request, and `User:{tenant}` for an unauthenticated request.
///
/// - [`AclSet::SecurityDisabled`] allows every write.
/// - An empty [`AclSet::Configured`] allows an unauthenticated write, and it
///   refuses an authenticated write. [`AclSet`] gives the reason, with the
///   evidence from the pinned Krabka broker.
/// - A non-empty [`AclSet::Configured`] allows the write only when an `Allow`
///   ACL matches the ACL principal and no `Deny` ACL matches it.
///
/// # Errors
///
/// Returns [`IngestLimitError::Unauthorized`] when the ACLs refuse the write.
pub(crate) fn check_tenant_wal_write_acl(
    principal: &Principal,
    tenant: &TenantId,
    wal_topic: &str,
    acls: &AclSet,
) -> Result<(), IngestLimitError> {
    let refusal = tenant_wal_acl_refusal(&TenantWalAclCheck {
        principal,
        tenant,
        wal_topic,
        acls,
        access: WalTopicAccess::Write,
    });
    refusal.map_or(Ok(()), |reason| {
        Err(IngestLimitError::Unauthorized {
            tenant: tenant.to_string(),
            reason,
        })
    })
}

#[derive(Clone, Debug, Default)]
pub struct InMemoryWalSink {
    pub(crate) records: Arc<Mutex<Vec<WalLogRecord>>>,
}

impl InMemoryWalSink {
    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn records(&self) -> Vec<WalLogRecord> {
        self.records.lock().expect("wal sink lock poisoned").clone()
    }
}

#[async_trait]
impl LogWalSink for InMemoryWalSink {
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError> {
        self.records
            .lock()
            .expect("wal sink lock poisoned")
            .push(record);
        Ok(())
    }
}

impl LogHotTail for InMemoryWalSink {
    fn records(&self) -> Vec<WalLogRecord> {
        InMemoryWalSink::records(self)
    }
}

/// Per-tenant byte token bucket for the `producer_byte_rate` ingest quota.
///
/// `updated_at` stays an [`Instant`] because it is a coordinate and not an
/// extent. The extent measured from it multiplies the rate into a byte
/// allowance.
#[derive(Debug)]
pub(crate) struct IngestQuotaBucket {
    pub(crate) rate: ByteRate,
    pub(crate) burst_window: Time,
    pub(crate) available: ByteSize,
    pub(crate) updated_at: Instant,
}

impl IngestQuotaBucket {
    pub(crate) fn new(rate: ByteRate, burst_window: Time) -> Self {
        Self {
            rate,
            burst_window,
            available: Self::burst_capacity(rate, burst_window),
            updated_at: Instant::now(),
        }
    }

    pub(crate) fn update_rate(&mut self, rate: ByteRate) {
        self.refill();
        self.rate = rate;
        // `>` is a permanent mutation survivor against `>=`: the two differ
        // only when the two are already equal, and then the assignment stores
        // the value already held.
        if self.available > self.capacity() {
            self.available = self.capacity();
        }
    }

    pub(crate) fn consume(&mut self, size: ByteSize) -> bool {
        self.refill();
        if size > self.available {
            return false;
        }
        self.available -= size;
        true
    }

    pub(crate) fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.updated_at).as_time();
        self.updated_at = now;
        // `ByteRate * Time` is a `ByteSize`, checked by the compiler.
        let refilled: ByteSize = (self.rate * elapsed).into();
        self.available = (self.available + refilled).min(self.capacity());
    }

    pub(crate) fn capacity(&self) -> ByteSize {
        Self::burst_capacity(self.rate, self.burst_window)
    }

    pub(crate) fn burst_capacity(rate: ByteRate, burst_window: Time) -> ByteSize {
        (rate * burst_window).into()
    }
}

pub trait LogHotTail: Send + Sync + 'static {
    fn records(&self) -> Vec<WalLogRecord>;

    /// Returns the hot-tail records whose `timestamp_ns` falls within the
    /// inclusive window `[start_ns, end_ns]`.
    ///
    /// Callers re-apply their exact per-record time bound downstream, so this
    /// may return a *superset* of the in-window records, for example records
    /// that share a coarse time bucket with the window edges. It MUST NOT drop
    /// any record whose timestamp lies in `[start_ns, end_ns]`. The default
    /// implementation filters [`LogHotTail::records`] and keeps its order.
    /// Implementations that hold a time index, see [`crate::BufferedLogHotTail`],
    /// override this to avoid a full-buffer scan.
    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        self.records()
            .into_iter()
            .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
            .collect()
    }

    /// Immutable records in the same inclusive window and order as
    /// [`Self::records_in_range`]. Buffered sources share their retained
    /// records; the default owns the existing range result.
    fn records_shared_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<Arc<WalLogRecord>> {
        self.records_in_range(start_ns, end_ns)
            .into_iter()
            .map(Arc::new)
            .collect()
    }
}

/// Decides whether a principal may write one push's records for a tenant.
///
/// Every record in `records` belongs to `tenant`. The distributor resolves the
/// tenant once per request and stamps it on every record it normalizes.
#[async_trait]
pub trait LogIngestLimiter: Send + Sync + 'static {
    /// Allows the push, or reports why it is refused or cannot be checked.
    ///
    /// The caller has already checked `principal` against its tenant grant.
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
        records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError>;

    /// Keeps the limiter's cached state fresh until `token` is cancelled.
    ///
    /// The role runs this as a supervised task. The default holds no cached
    /// state, so it only waits for the cancel.
    async fn keep_fresh(&self, token: CancellationToken) {
        token.cancelled().await;
    }
}

/// Decides whether a principal may read the logs of a tenant.
///
/// The querier asks before every read of a tenant's data, and the ruler and
/// the delete-request API ask before they read or change a tenant's rule
/// groups or delete requests. An implementation is called from many requests
/// at once, so it should answer from memory and not wait on a remote call per
/// request.
#[async_trait]
pub trait LogQueryAuthorizer: Send + Sync + 'static {
    /// Allows `principal` to read `tenant`, or reports why it is refused or
    /// cannot be checked.
    ///
    /// The caller has already checked `principal` against its tenant grant.
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError>;
}

#[async_trait]
pub trait LogWalConsumer: Send + 'static {
    fn take_revoked_partitions(&mut self) -> std::collections::BTreeSet<i32> {
        std::collections::BTreeSet::new()
    }

    /// Connects readiness to consumers that can report broker catch-up.
    fn set_catch_up_gate(&mut self, gate: ReadinessGate) {
        gate.mark_ready();
    }

    async fn poll(&mut self, timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError>;

    /// Whether an empty poll means every record has been durably applied.
    /// Remote consumers must confirm this against the current log end: an
    /// empty poll can simply mean a fetch or assignment timed out.
    /// Finite in-memory consumers may use the default.
    async fn is_drained(&mut self) -> bool {
        true
    }

    /// Reports that the last polled batch was applied successfully.
    async fn records_applied(&mut self) {}

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError>;
}

#[async_trait]
pub trait LogWalSink: Send + Sync + 'static {
    /// Appends one record and waits for its ack.
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError>;

    /// Appends one push request's records as a batch.
    ///
    /// A Loki push becomes one record per entry, so this is the call that
    /// decides how many broker round trips one push costs. The default appends
    /// serially and stops at the first failure, which is all a sink with no
    /// produce pipeline can do. [`crate::KafkaLogWalSink`] overrides it.
    ///
    /// # Errors
    /// Returns [`WalBatchError`] when a record fails to append. The error
    /// carries the count of records that did append, so the handler can report
    /// a partial batch as the partial batch it is.
    async fn append_batch(
        &self,
        records: Vec<WalLogRecord>,
    ) -> Result<(), WalBatchError<WalSinkError>> {
        write_batch_serially(records, |record| self.append(record)).await
    }
}

/// A [`LogQueryAuthorizer`] whose underlying implementation can change after
/// construction.
///
/// The querier uses it to fail closed while the real
/// [`BrokerBackedQueryAuthorizer`] connects asynchronously.
pub(crate) struct SwappableQueryAuthorizer {
    pub(crate) inner: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
}

pub(crate) struct SwappableQueryAuthorizerParts {
    pub(crate) authorizer: SwappableQueryAuthorizer,
    pub(crate) slot: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
}

impl SwappableQueryAuthorizer {
    /// Creates a new swappable authorizer that starts unavailable.
    pub(crate) fn unavailable() -> SwappableQueryAuthorizerParts {
        let inner: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>> = Arc::new(
            tokio::sync::RwLock::new(Arc::new(UnavailableQueryAuthorizer)),
        );
        SwappableQueryAuthorizerParts {
            authorizer: Self {
                inner: inner.clone(),
            },
            slot: inner,
        }
    }
}

#[async_trait]
impl LogQueryAuthorizer for SwappableQueryAuthorizer {
    async fn check(
        &self,
        principal: &Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        let authorizer = self.inner.read().await.clone();
        authorizer.check(principal, tenant).await
    }
}

#[derive(Debug)]
pub(crate) struct TenantLruEntry<V> {
    pub(crate) last_used: u64,
    pub(crate) value: V,
}

/// A per-tenant map that holds at most `capacity` tenants.
///
/// Every read and every insert marks the tenant as the most recently used. An
/// insert past the capacity first removes the least recently used tenant. The
/// map therefore cannot grow with the number of tenant names a client sends.
#[derive(Debug)]
pub(crate) struct TenantLru<V> {
    pub(crate) capacity: NonZeroUsize,
    pub(crate) entries: BTreeMap<TenantId, TenantLruEntry<V>>,
    pub(crate) recency: BTreeMap<u64, TenantId>,
    pub(crate) clock: u64,
}

impl<V> TenantLru<V> {
    pub(crate) fn new(capacity: NonZeroUsize) -> Self {
        Self {
            capacity,
            entries: BTreeMap::new(),
            recency: BTreeMap::new(),
            clock: 0,
        }
    }

    /// The tenant's value, marked as the most recently used.
    pub(crate) fn get_mut(&mut self, tenant: &TenantId) -> Option<&mut V> {
        self.clock += 1;
        let tick = self.clock;
        let entry = self.entries.get_mut(tenant)?;
        self.recency.remove(&entry.last_used);
        entry.last_used = tick;
        self.recency.insert(tick, tenant.clone());
        Some(&mut entry.value)
    }

    /// The tenant's value, inserted from `make` when absent, and marked as the
    /// most recently used.
    pub(crate) fn get_or_insert_with(
        &mut self,
        tenant: &TenantId,
        make: impl FnOnce() -> V,
    ) -> &mut V {
        if !self.entries.contains_key(tenant)
            && self.entries.len() >= self.capacity.get()
            && let Some(evicted_entry) = self.recency.pop_first()
        {
            self.entries.remove(&evicted_entry.1);
        }
        self.clock += 1;
        let tick = self.clock;
        let entry = self
            .entries
            .entry(tenant.clone())
            .or_insert_with(|| TenantLruEntry {
                last_used: tick,
                value: make(),
            });
        self.recency.remove(&entry.last_used);
        entry.last_used = tick;
        self.recency.insert(tick, tenant.clone());
        &mut entry.value
    }
}

/// The WAL topic operation a principal asks the ACLs for.
#[derive(Clone, Copy)]
pub(crate) enum WalTopicAccess {
    Read,
    Write,
}

impl WalTopicAccess {
    fn verb(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// One ACL decision: may `principal` perform `access` on `wal_topic` for
/// `tenant` under `acls`?
pub(crate) struct TenantWalAclCheck<'a> {
    pub(crate) principal: &'a Principal,
    pub(crate) tenant: &'a TenantId,
    pub(crate) wal_topic: &'a str,
    pub(crate) acls: &'a AclSet,
    pub(crate) access: WalTopicAccess,
}

/// Decides a WAL topic ACL check, returning the refusal reason when the ACLs
/// refuse it. [`check_tenant_wal_read_acl`](check_tenant_wal_read_acl)
/// documents the rules.
pub(crate) fn tenant_wal_acl_refusal(check: &TenantWalAclCheck<'_>) -> Option<String> {
    let TenantWalAclCheck {
        principal,
        tenant,
        wal_topic,
        acls,
        access,
    } = *check;
    let entries = match acls {
        AclSet::SecurityDisabled => return None,
        AclSet::Configured(entries)
            if entries.is_empty() && matches!(principal, Principal::Unauthenticated) =>
        {
            return None;
        }
        AclSet::Configured(entries) => entries,
    };
    let acl_principal = principal.acl_principal(tenant);
    let verb = access.verb();
    let mut allowed = false;
    for acl in entries {
        let matches = match access {
            WalTopicAccess::Read => acl_matches_tenant_wal_read(acl, &acl_principal, wal_topic),
            WalTopicAccess::Write => acl_matches_tenant_wal_write(acl, &acl_principal, wal_topic),
        };
        if !matches {
            continue;
        }
        match acl.permission_type {
            PermissionType::Deny => {
                return Some(format!(
                    "tenant {verb} ACL denied for WAL topic `{wal_topic}`"
                ));
            }
            PermissionType::Allow => allowed = true,
        }
    }

    (!allowed).then(|| format!("missing tenant {verb} ACL for WAL topic `{wal_topic}`"))
}

#[derive(Clone, Debug, Default)]
pub(crate) struct UnavailableQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for UnavailableQueryAuthorizer {
    async fn check(
        &self,
        _principal: &Principal,
        tenant: &TenantId,
    ) -> Result<(), QueryAuthorizationError> {
        Err(QueryAuthorizationError::Unavailable {
            tenant: tenant.to_string(),
            reason: "broker-backed query authorization is not connected".to_string(),
        })
    }
}
