use super::*;

#[async_trait]
pub trait LogWalSink: Send + Sync + 'static {
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError>;
}

#[async_trait]
pub trait LogIngestLimiter: Send + Sync + 'static {
    async fn check(&self, tenant: &str, records: &[WalLogRecord]) -> Result<(), IngestLimitError>;
}

#[async_trait]
pub trait LogQueryAuthorizer: Send + Sync + 'static {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError>;
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
    /// Implementations that hold a time index, see [`BufferedLogHotTail`],
    /// override this to avoid a full-buffer scan.
    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        self.records()
            .into_iter()
            .filter(|record| record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns)
            .collect()
    }
}

#[async_trait]
pub trait LogWalConsumer: Send + 'static {
    async fn poll(&mut self, timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError>;

    async fn commit_compacted(&mut self, position: WalPosition) -> Result<(), WalConsumerError>;
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

#[derive(Clone, Debug, Default)]
pub(crate) struct AllowAllIngestLimiter;

#[async_trait]
impl LogIngestLimiter for AllowAllIngestLimiter {
    async fn check(
        &self,
        _tenant: &str,
        _records: &[WalLogRecord],
    ) -> Result<(), IngestLimitError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct AllowAllQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for AllowAllQueryAuthorizer {
    async fn check(&self, _tenant: &str) -> Result<(), QueryAuthorizationError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct UnavailableQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for UnavailableQueryAuthorizer {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        Err(QueryAuthorizationError::Unavailable {
            tenant: tenant.to_string(),
            reason: "broker-backed query authorization is not connected".to_string(),
        })
    }
}

pub(crate) struct BrokerBackedQueryAuthorizer {
    pub(crate) admin: tokio::sync::Mutex<AdminClient>,
    pub(crate) wal_topic: String,
    pub(crate) connected: Arc<AtomicBool>,
}

impl BrokerBackedQueryAuthorizer {
    pub(crate) async fn connect(
        bootstrap: &str,
        wal_topic: String,
        client_resource_policy: ClientResourcePolicy,
        connected: Arc<AtomicBool>,
    ) -> Result<Self, AdminError> {
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            admin_connection_options(client_resource_policy),
        )
        .await?;
        Ok(Self {
            admin: tokio::sync::Mutex::new(admin),
            wal_topic,
            connected,
        })
    }
}

#[async_trait]
impl LogQueryAuthorizer for BrokerBackedQueryAuthorizer {
    #[cfg_attr(test, mutants::skip)]
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        let result = {
            let mut admin = self.admin.lock().await;
            admin.describe_acls(&AclEntryFilter::default()).await
        };
        let acls = match result {
            Ok(acls) => {
                self.connected.store(true, AtomicOrdering::SeqCst);
                acls
            }
            Err(error) => {
                self.connected.store(false, AtomicOrdering::SeqCst);
                return Err(QueryAuthorizationError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                });
            }
        };
        check_tenant_wal_read_acl(tenant, &self.wal_topic, &acls)
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

impl SwappableQueryAuthorizer {
    /// Creates a new swappable authorizer that starts unavailable.
    pub(crate) fn new() -> (Self, Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>) {
        let inner: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>> = Arc::new(
            tokio::sync::RwLock::new(Arc::new(UnavailableQueryAuthorizer)),
        );
        (
            Self {
                inner: inner.clone(),
            },
            inner,
        )
    }
}

#[async_trait]
impl LogQueryAuthorizer for SwappableQueryAuthorizer {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        let authorizer = self.inner.read().await.clone();
        authorizer.check(tenant).await
    }
}

pub(crate) const PRODUCER_BYTE_RATE_QUOTA_KEY: &str = "producer_byte_rate";

pub(crate) struct BrokerBackedIngestLimiter {
    pub(crate) admin: tokio::sync::Mutex<AdminClient>,
    pub(crate) wal_topic: String,
    pub(crate) burst_window: Time,
    pub(crate) buckets: Mutex<BTreeMap<String, IngestQuotaBucket>>,
}

impl BrokerBackedIngestLimiter {
    pub(crate) async fn connect(
        bootstrap: &str,
        wal_topic: String,
        client_resource_policy: ClientResourcePolicy,
        burst_window: Time,
    ) -> Result<Self, AdminError> {
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            admin_connection_options(client_resource_policy),
        )
        .await?;
        Ok(Self {
            admin: tokio::sync::Mutex::new(admin),
            wal_topic,
            burst_window,
            buckets: Mutex::new(BTreeMap::new()),
        })
    }
}

pub(crate) fn admin_connection_options(
    client_resource_policy: ClientResourcePolicy,
) -> krabka_client_core::ConnectionOptions {
    krabka_client_core::ConnectionOptions {
        dispatch_queue_capacity: client_resource_policy.dispatch_queue_capacity,
        frame_max: client_resource_policy.frame_max,
        ..krabka_client_core::ConnectionOptions::default()
    }
}

#[async_trait]
impl LogIngestLimiter for BrokerBackedIngestLimiter {
    #[cfg_attr(test, mutants::skip)]
    async fn check(&self, tenant: &str, records: &[WalLogRecord]) -> Result<(), IngestLimitError> {
        let (acls, quota) = {
            let mut admin = self.admin.lock().await;
            let acls = admin
                .describe_acls(&AclEntryFilter::default())
                .await
                .map_err(|error| IngestLimitError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                })?;
            let quota = admin.describe_user_quotas(tenant).await.map_err(|error| {
                IngestLimitError::Unavailable {
                    tenant: tenant.to_string(),
                    reason: error.to_string(),
                }
            })?;
            (acls, quota)
        };
        check_tenant_wal_write_acl(tenant, &self.wal_topic, &acls)?;

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
        let bucket = buckets
            .entry(tenant.to_string())
            .or_insert_with(|| IngestQuotaBucket::new(rate, self.burst_window));
        bucket.update_rate(rate);
        if bucket.consume(batch) {
            return Ok(());
        }

        let (rate, bytes) = (rate.bytes_per_sec_f64(), batch.bytes_usize());
        Err(IngestLimitError::RateLimited {
            tenant: tenant.to_string(),
            reason: format!(
                "{PRODUCER_BYTE_RATE_QUOTA_KEY} quota {rate:.0} bytes/s exceeded by {bytes} byte ingest batch"
            ),
        })
    }
}

pub(crate) fn check_tenant_wal_write_acl(
    tenant: &str,
    wal_topic: &str,
    acls: &[AclEntry],
) -> Result<(), IngestLimitError> {
    if acls.is_empty() {
        return Ok(());
    }

    let principal = format!("User:{tenant}");
    let mut allowed = false;
    for acl in acls {
        if !acl_matches_tenant_wal_write(acl, &principal, wal_topic) {
            continue;
        }
        match acl.permission_type {
            PermissionType::Deny => {
                return Err(IngestLimitError::Unauthorized {
                    tenant: tenant.to_string(),
                    reason: format!("tenant write ACL denied for WAL topic `{wal_topic}`"),
                });
            }
            PermissionType::Allow => allowed = true,
        }
    }

    if allowed {
        Ok(())
    } else {
        Err(IngestLimitError::Unauthorized {
            tenant: tenant.to_string(),
            reason: format!("missing tenant write ACL for WAL topic `{wal_topic}`"),
        })
    }
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

pub(crate) fn check_tenant_wal_read_acl(
    tenant: &str,
    wal_topic: &str,
    acls: &[AclEntry],
) -> Result<(), QueryAuthorizationError> {
    if acls.is_empty() {
        return Ok(());
    }

    let principal = format!("User:{tenant}");
    let mut allowed = false;
    for acl in acls {
        if !acl_matches_tenant_wal_read(acl, &principal, wal_topic) {
            continue;
        }
        match acl.permission_type {
            PermissionType::Deny => {
                return Err(QueryAuthorizationError::Unauthorized {
                    tenant: tenant.to_string(),
                    reason: format!("tenant read ACL denied for WAL topic `{wal_topic}`"),
                });
            }
            PermissionType::Allow => allowed = true,
        }
    }

    if allowed {
        Ok(())
    } else {
        Err(QueryAuthorizationError::Unauthorized {
            tenant: tenant.to_string(),
            reason: format!("missing tenant read ACL for WAL topic `{wal_topic}`"),
        })
    }
}

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

pub(crate) fn matches_acl_topic_pattern(acl: &AclEntry, wal_topic: &str) -> bool {
    match acl.pattern_type {
        PatternType::Literal => acl.resource_name == wal_topic || acl.resource_name == "*",
        PatternType::Prefixed => wal_topic.starts_with(&acl.resource_name),
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
                        .map(|(name, value)| name.len() + value.len())
                        .sum::<usize>()
                    + record
                        .structured_metadata
                        .iter()
                        .map(|(name, value)| name.len() + value.len())
                        .sum::<usize>()
            })
            .sum(),
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
