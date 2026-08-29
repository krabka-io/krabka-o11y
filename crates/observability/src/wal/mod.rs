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
    records: Arc<Mutex<Vec<WalLogRecord>>>,
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
struct AllowAllIngestLimiter;

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
struct AllowAllQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for AllowAllQueryAuthorizer {
    async fn check(&self, _tenant: &str) -> Result<(), QueryAuthorizationError> {
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
struct UnavailableQueryAuthorizer;

#[async_trait]
impl LogQueryAuthorizer for UnavailableQueryAuthorizer {
    async fn check(&self, tenant: &str) -> Result<(), QueryAuthorizationError> {
        Err(QueryAuthorizationError::Unavailable {
            tenant: tenant.to_string(),
            reason: "broker-backed query authorization is not connected".to_string(),
        })
    }
}

struct BrokerBackedQueryAuthorizer {
    admin: tokio::sync::Mutex<AdminClient>,
    wal_topic: String,
    connected: Arc<AtomicBool>,
}

impl BrokerBackedQueryAuthorizer {
    async fn connect(
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
struct SwappableQueryAuthorizer {
    inner: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
}

impl SwappableQueryAuthorizer {
    /// Creates a new swappable authorizer that starts unavailable.
    fn new() -> (Self, Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>) {
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

const PRODUCER_BYTE_RATE_QUOTA_KEY: &str = "producer_byte_rate";

struct BrokerBackedIngestLimiter {
    admin: tokio::sync::Mutex<AdminClient>,
    wal_topic: String,
    burst_window: Time,
    buckets: Mutex<BTreeMap<String, IngestQuotaBucket>>,
}

impl BrokerBackedIngestLimiter {
    async fn connect(
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

fn admin_connection_options(
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

fn check_tenant_wal_write_acl(
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

fn acl_matches_tenant_wal_write(acl: &AclEntry, principal: &str, wal_topic: &str) -> bool {
    acl.resource_type == ResourceType::Topic
        && matches!(acl.operation, AclOperation::All | AclOperation::Write)
        && (acl.principal == principal || acl.principal == "User:*")
        && matches_acl_topic_pattern(acl, wal_topic)
}

fn check_tenant_wal_read_acl(
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

fn acl_matches_tenant_wal_read(acl: &AclEntry, principal: &str, wal_topic: &str) -> bool {
    acl.resource_type == ResourceType::Topic
        && matches!(acl.operation, AclOperation::All | AclOperation::Read)
        && (acl.principal == principal || acl.principal == "User:*")
        && matches_acl_topic_pattern(acl, wal_topic)
}

fn matches_acl_topic_pattern(acl: &AclEntry, wal_topic: &str) -> bool {
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
struct IngestQuotaBucket {
    rate: ByteRate,
    burst_window: Time,
    available: ByteSize,
    updated_at: Instant,
}

impl IngestQuotaBucket {
    fn new(rate: ByteRate, burst_window: Time) -> Self {
        Self {
            rate,
            burst_window,
            available: Self::burst_capacity(rate, burst_window),
            updated_at: Instant::now(),
        }
    }

    fn update_rate(&mut self, rate: ByteRate) {
        self.refill();
        self.rate = rate;
        // `>` is a permanent mutation survivor against `>=`: the two differ
        // only when the two are already equal, and then the assignment stores
        // the value already held.
        if self.available > self.capacity() {
            self.available = self.capacity();
        }
    }

    fn consume(&mut self, size: ByteSize) -> bool {
        self.refill();
        if size > self.available {
            return false;
        }
        self.available -= size;
        true
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.updated_at).as_time();
        self.updated_at = now;
        // `ByteRate * Time` is a `ByteSize`, checked by the compiler.
        let refilled: ByteSize = (self.rate * elapsed).into();
        self.available = (self.available + refilled).min(self.capacity());
    }

    fn capacity(&self) -> ByteSize {
        Self::burst_capacity(self.rate, self.burst_window)
    }

    fn burst_capacity(rate: ByteRate, burst_window: Time) -> ByteSize {
        (rate * burst_window).into()
    }
}

fn ingest_quota_bytes(records: &[WalLogRecord]) -> ByteSize {
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
fn hot_tail_bucket_key(timestamp_ns: i64, bucket_width: Time) -> i64 {
    timestamp_ns.div_euclid(bucket_width.nanos_i64())
}

/// Buffer holding polled hot-tail records.
///
/// Records arrive from Kafka polling in NO timestamp order, so the buffer keeps
/// two views of the same data:
///
/// * `records` is the append-ordered log. [`records`](Self::records) clones
///   this whole list. It backs the WebSocket tail path, which indexes into the
///   buffer by arrival order and must see every record.
/// * `buckets` is a per-minute time index. It maps each
///   [`hot_tail_bucket_key`] to the positions in `records` whose timestamps
///   land in that bucket. An out-of-order arrival lands in the correct bucket,
///   and no global sort is ever needed.
///
/// [`records_in_range`](Self::records_in_range) walks only the buckets that
/// overlap the query window, so a 30-minute query over a buffer that holds
/// hours of logs touches only the window's records, instead of a scan of the
/// entire buffer.
#[derive(Debug)]
struct HotTailBuffer {
    bucket_width: Time,
    records: Vec<WalLogRecord>,
    buckets: BTreeMap<i64, Vec<usize>>,
}

impl HotTailBuffer {
    fn push(&mut self, record: WalLogRecord) {
        let index = self.records.len();
        let bucket = hot_tail_bucket_key(record.timestamp_ns, self.bucket_width);
        self.records.push(record);
        self.buckets.entry(bucket).or_default().push(index);
    }

    fn prune_compacted(&mut self, frontier: &CompactionFrontier) -> usize {
        let before = self.records.len();
        if before == 0 {
            return 0;
        }

        let old_records = std::mem::take(&mut self.records);
        self.records = old_records
            .into_iter()
            .filter(|record| !frontier.is_compacted(record))
            .collect();
        let pruned = before - self.records.len();
        // `> 0` is a permanent survivor against `>= 0`: rebuilding the bucket
        // index from an unchanged record list produces the index already held,
        // and shrinking spare capacity is not observable.
        if pruned > 0 {
            self.records.shrink_to_fit();
            self.rebuild_buckets();
        }
        pruned
    }

    fn rebuild_buckets(&mut self) {
        self.buckets.clear();
        for (index, record) in self.records.iter().enumerate() {
            self.buckets
                .entry(hot_tail_bucket_key(record.timestamp_ns, self.bucket_width))
                .or_default()
                .push(index);
        }
        for indices in self.buckets.values_mut() {
            indices.shrink_to_fit();
        }
    }

    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        if start_ns > end_ns {
            return Vec::new();
        }
        let start_bucket = hot_tail_bucket_key(start_ns, self.bucket_width);
        let end_bucket = hot_tail_bucket_key(end_ns, self.bucket_width);
        let mut matches: Vec<usize> = Vec::new();
        for (_bucket, indices) in self.buckets.range(start_bucket..=end_bucket) {
            for &index in indices {
                let record = &self.records[index];
                if record.timestamp_ns >= start_ns && record.timestamp_ns <= end_ns {
                    matches.push(index);
                }
            }
        }
        // Restore append order so the windowed slice matches a full-buffer scan
        // exactly (downstream collects into a BTreeMap and re-sorts, so order is not
        // load-bearing, but matching the full-scan order keeps the two paths trivially
        // equivalent for testing and reasoning).
        matches.sort_unstable();
        matches
            .into_iter()
            .map(|index| self.records[index].clone())
            .collect()
    }
}

impl Default for HotTailBuffer {
    fn default() -> Self {
        Self {
            bucket_width: minutes(1),
            records: Vec::new(),
            buckets: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct BufferedLogHotTail {
    buffer: Arc<Mutex<HotTailBuffer>>,
}

impl BufferedLogHotTail {
    // Both mutations of this function are permanent survivors, and both are
    // equivalent: `HotTailBuffer::default()` already carries a one-minute
    // width, and the width is an index granularity that push and query both
    // read from the same field. Whatever it is, the two stay consistent and no
    // record is found or lost because of it.
    fn with_bucket_width(bucket_width: Time) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(HotTailBuffer {
                bucket_width,
                ..HotTailBuffer::default()
            })),
        }
    }
    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn records(&self) -> Vec<WalLogRecord> {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .records
            .clone()
    }

    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .records_in_range(start_ns, end_ns)
    }

    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn append_records(&self, records: Vec<WalLogRecord>) {
        let mut buffer = self.buffer.lock().expect("hot tail buffer lock poisoned");
        for record in records {
            buffer.push(record);
        }
    }

    #[must_use]
    /// # Panics
    /// Panics if synchronized telemetry state is poisoned or validated columnar data is missing a required field.
    pub fn prune_compacted(&self, frontier: &CompactionFrontier) -> usize {
        self.buffer
            .lock()
            .expect("hot tail buffer lock poisoned")
            .prune_compacted(frontier)
    }
}

impl LogHotTail for BufferedLogHotTail {
    fn records(&self) -> Vec<WalLogRecord> {
        BufferedLogHotTail::records(self)
    }

    fn records_in_range(&self, start_ns: i64, end_ns: i64) -> Vec<WalLogRecord> {
        BufferedLogHotTail::records_in_range(self, start_ns, end_ns)
    }
}

#[derive(Clone)]
pub struct KafkaLogWalSink {
    producer: Arc<Producer>,
    topic: String,
}

impl KafkaLogWalSink {
    #[must_use]
    pub fn new(producer: Producer, topic: impl Into<String>) -> Self {
        Self {
            producer: Arc::new(producer),
            topic: topic.into(),
        }
    }

    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    pub async fn connect(
        bootstrap: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ProducerError> {
        Self::connect_with_client_resource_policy(bootstrap, topic, ClientResourcePolicy::default())
            .await
    }

    /// Connects with the supplied validated Kafka connection limits.
    ///
    /// # Errors
    /// Returns an error when the producer cannot start.
    pub async fn connect_with_client_resource_policy(
        bootstrap: impl Into<String>,
        topic: impl Into<String>,
        client_resource_policy: ClientResourcePolicy,
    ) -> Result<Self, ProducerError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap)
            .client_id("krabka-observability-distributor")
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self::new(producer, topic))
    }
}

#[async_trait]
impl LogWalSink for KafkaLogWalSink {
    #[cfg_attr(test, mutants::skip)]
    async fn append(&self, record: WalLogRecord) -> Result<(), WalSinkError> {
        let delivery = self
            .producer
            .send(build_kafka_wal_record(&self.topic, &record)?)
            .await;
        delivery
            .await
            .map_err(|_| WalSinkError::DeliveryCanceled)??;
        Ok(())
    }
}

pub struct KafkaLogWalConsumer {
    consumer: Consumer,
}

impl KafkaLogWalConsumer {
    #[cfg_attr(test, mutants::skip)]
    /// # Errors
    /// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
    pub async fn connect(
        bootstrap: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
    ) -> Result<Self, ConsumerError> {
        Self::connect_with_client_resource_policy(
            bootstrap,
            group_id,
            topic,
            ClientResourcePolicy::default(),
        )
        .await
    }

    /// Connects with the supplied validated Kafka connection limits.
    ///
    /// # Errors
    /// Returns an error when the consumer cannot start.
    pub async fn connect_with_client_resource_policy(
        bootstrap: impl Into<String>,
        group_id: impl Into<String>,
        topic: impl Into<String>,
        client_resource_policy: ClientResourcePolicy,
    ) -> Result<Self, ConsumerError> {
        let topic = topic.into();
        let consumer = Consumer::builder()
            .bootstrap(bootstrap)
            .client_id("krabka-observability-compactor")
            .dispatch_queue_capacity(client_resource_policy.dispatch_queue_capacity.get())
            .frame_max(client_resource_policy.frame_max.size())
            .group_id(group_id)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .subscribe(vec![topic])
            .build()
            .await?;
        Ok(Self { consumer })
    }

    #[cfg_attr(test, mutants::skip)]
    pub(crate) async fn close(self) {
        let _ = self.consumer.close().await;
    }
}

#[async_trait]
impl LogWalConsumer for KafkaLogWalConsumer {
    #[cfg_attr(test, mutants::skip)]
    async fn poll(&mut self, timeout: Time) -> Result<Vec<KafkaWalRecord>, WalConsumerError> {
        self.consumer
            .poll(timeout)
            .await?
            .into_iter()
            .map(|record| {
                let value = record
                    .value
                    .ok_or_else(|| WalConsumerError::MissingValue {
                        topic: record.topic.clone(),
                        partition: record.partition,
                        offset: record.offset,
                    })?
                    .to_vec();
                Ok(KafkaWalRecord {
                    value,
                    partition: PartitionIndex(record.partition),
                    offset: Offset(record.offset),
                    timestamp_ms: Some(record.timestamp),
                    headers: record
                        .headers
                        .into_iter()
                        .map(|header| KafkaWalHeader {
                            key: header.key,
                            value: header.value.map(|value| value.to_vec()),
                        })
                        .collect(),
                })
            })
            .collect()
    }

    #[cfg_attr(test, mutants::skip)]
    async fn commit_compacted(&mut self, _position: WalPosition) -> Result<(), WalConsumerError> {
        self.consumer.commit_sync().await?;
        Ok(())
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn build_kafka_wal_record(
    topic: impl Into<String>,
    record: &WalLogRecord,
) -> Result<ProducerRecord, WalSinkError> {
    let fingerprint = series_fingerprint(&record.labels);
    let mut headers = vec![
        ProducerHeader {
            key: "krabka-wal-record-type".to_string(),
            value: Some(Bytes::from_static(b"log")),
        },
        ProducerHeader {
            key: "krabka-tenant".to_string(),
            value: Some(Bytes::from(record.tenant.clone())),
        },
    ];
    // Inject the current span's W3C trace context (`traceparent`/`tracestate`)
    // so the compactor can stitch its consume/compaction span onto the ingest
    // trace. Additive: the record body is unchanged, and this is a no-op when
    // there is no active/sampled span.
    for (key, value) in krabka_telemetry::propagation::current_trace_headers() {
        headers.push(ProducerHeader {
            key,
            value: Some(Bytes::from(value.into_bytes())),
        });
    }
    Ok(ProducerRecord {
        topic: topic.into(),
        partition: None,
        key: Some(Bytes::from(format!("{}:{fingerprint}", record.tenant))),
        value: Some(Bytes::from(serde_json::to_vec(record)?)),
        headers,
        timestamp_ms: Some(record.timestamp_ns / 1_000_000),
    })
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn decode_kafka_wal_record(
    value: &[u8],
    partition: PartitionIndex,
    offset: Offset,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    let mut record: WalLogRecord = serde_json::from_slice(value)?;
    record.position = Some(WalPosition { partition, offset });
    Ok(record)
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub fn decode_kafka_wal_record_envelope(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    match decode_kafka_wal_record(&record.value, record.partition, record.offset) {
        Ok(record) => Ok(record),
        Err(_) if has_native_kafka_log_headers(&record.headers) => {
            decode_native_kafka_log_record(record)
        }
        Err(error) => Err(error),
    }
}

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn poll_log_hot_tail_once(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    hot_tail: &BufferedLogHotTail,
    timeout: Time,
) -> Result<usize, HotTailPollError> {
    poll_log_hot_tail_once_with_frontier(consumer, hot_tail, timeout, None).await
}

async fn poll_log_hot_tail_once_with_frontier(
    consumer: &mut (impl LogWalConsumer + ?Sized),
    hot_tail: &BufferedLogHotTail,
    timeout: Time,
    frontier: Option<&SharedCompactionFrontier>,
) -> Result<usize, HotTailPollError> {
    let batch = consumer.poll(timeout).await?;
    let records = batch
        .into_iter()
        .map(decode_kafka_wal_record_envelope)
        .collect::<Result<Vec<_>, _>>()?;
    let decoded = records.len();
    hot_tail.append_records(records);
    if let Some(frontier) = frontier {
        let _ = hot_tail.prune_compacted(&frontier.snapshot());
    }
    Ok(decoded)
}

#[cfg_attr(test, mutants::skip)]
fn spawn_log_hot_tail_poller(
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
fn spawn_wal_hot_tail_connect_and_poll(
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

/// Spawns a background task that retries `BrokerBackedQueryAuthorizer::connect`
/// until it succeeds, then swaps the unavailable authorizer for the real
/// broker-backed authorizer.
#[cfg_attr(test, mutants::skip)]
fn spawn_query_authorizer_connect(
    bootstrap: String,
    topic: String,
    slot: Arc<tokio::sync::RwLock<Arc<dyn LogQueryAuthorizer>>>,
    client_resource_policy: ClientResourcePolicy,
    reconnect_interval: Time,
    token: CancellationToken,
    readiness: ServiceReadiness,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let authorizer = loop {
            let result = tokio::select! {
                () = token.cancelled() => return,
                result = BrokerBackedQueryAuthorizer::connect(
                &bootstrap,
                topic.clone(),
                client_resource_policy,
                readiness.authorization_connected.clone(),
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
        readiness
            .authorization_connected
            .store(true, AtomicOrdering::SeqCst);
        tracing::info!("querier query authorizer connected; broker-backed ACL checks active");
        token.cancelled().await;
    })
}

fn has_native_kafka_log_headers(headers: &[KafkaWalHeader]) -> bool {
    headers.iter().any(|header| {
        header.key == "krabka-log-timestamp-ns"
            || header.key.starts_with("krabka-log-label-")
            || (header.key == "krabka-wal-record-type"
                && header
                    .value
                    .as_deref()
                    .is_some_and(|value| value == b"log-line"))
    })
}

fn decode_native_kafka_log_record(
    record: KafkaWalRecord,
) -> Result<WalLogRecord, WalRecordDecodeError> {
    let tenant = required_kafka_header_utf8(&record.headers, "krabka-tenant")?;
    let timestamp_ns = if let Some(value) =
        optional_kafka_header_utf8(&record.headers, "krabka-log-timestamp-ns")?
    {
        let timestamp_ns =
            value
                .parse()
                .map_err(|source| WalRecordDecodeError::InvalidNativeTimestamp {
                    value: value.clone(),
                    source,
                })?;
        validate_native_timestamp_ns(timestamp_ns, value)?
    } else {
        let timestamp_ms =
            record
                .timestamp_ms
                .ok_or_else(|| WalRecordDecodeError::MissingNativeHeader {
                    name: "krabka-log-timestamp-ns".to_string(),
                })?;
        native_timestamp_ms_to_ns(timestamp_ms)?
    };
    let labels = kafka_headers_with_prefix(&record.headers, "krabka-log-label-", |name| {
        WalRecordDecodeError::DuplicateNativeLabelName { name }
    })?;
    if labels.is_empty() {
        return Err(WalRecordDecodeError::MissingNativeLabels);
    }
    if let Some(name) = labels.keys().find(|name| !is_loki_label_name(name)) {
        return Err(WalRecordDecodeError::InvalidNativeLabelName { name: name.clone() });
    }
    let structured_metadata =
        kafka_headers_with_prefix(&record.headers, "krabka-log-metadata-", |name| {
            WalRecordDecodeError::DuplicateNativeMetadataName { name }
        })?;
    if let Some(name) = structured_metadata
        .keys()
        .find(|name| !is_loki_label_name(name))
    {
        return Err(WalRecordDecodeError::InvalidNativeMetadataName { name: name.clone() });
    }
    let line = String::from_utf8(record.value)
        .map_err(|_| WalRecordDecodeError::InvalidNativeLogLineUtf8)?;

    Ok(WalLogRecord {
        tenant,
        labels,
        timestamp_ns,
        line,
        structured_metadata,
        position: Some(WalPosition {
            partition: record.partition,
            offset: record.offset,
        }),
    })
}

fn required_kafka_header_utf8(
    headers: &[KafkaWalHeader],
    name: &str,
) -> Result<String, WalRecordDecodeError> {
    optional_kafka_header_utf8(headers, name)?.ok_or_else(|| {
        WalRecordDecodeError::MissingNativeHeader {
            name: name.to_string(),
        }
    })
}

fn optional_kafka_header_utf8(
    headers: &[KafkaWalHeader],
    name: &str,
) -> Result<Option<String>, WalRecordDecodeError> {
    let Some(header) = headers.iter().find(|header| header.key == name) else {
        return Ok(None);
    };
    let value =
        header
            .value
            .as_ref()
            .ok_or_else(|| WalRecordDecodeError::MissingNativeHeaderValue {
                name: name.to_string(),
            })?;
    String::from_utf8(value.clone()).map(Some).map_err(|_| {
        WalRecordDecodeError::InvalidNativeHeaderUtf8 {
            name: name.to_string(),
        }
    })
}

fn native_timestamp_ms_to_ns(timestamp_ms: i64) -> Result<i64, WalRecordDecodeError> {
    let converted_ns = timestamp_ms.checked_mul(1_000_000).ok_or_else(|| {
        WalRecordDecodeError::InvalidNativeTimestampValue {
            value: timestamp_ms.to_string(),
        }
    })?;
    validate_native_timestamp_ns(converted_ns, timestamp_ms.to_string())
}

fn validate_native_timestamp_ns(
    timestamp_ns: i64,
    value: String,
) -> Result<i64, WalRecordDecodeError> {
    if timestamp_ns < 0 {
        Err(WalRecordDecodeError::InvalidNativeTimestampValue { value })
    } else {
        Ok(timestamp_ns)
    }
}

fn kafka_headers_with_prefix(
    headers: &[KafkaWalHeader],
    prefix: &str,
    duplicate_error: impl Fn(String) -> WalRecordDecodeError,
) -> Result<BTreeMap<String, String>, WalRecordDecodeError> {
    let mut values = BTreeMap::new();
    for header in headers {
        let Some(name) = header.key.strip_prefix(prefix) else {
            continue;
        };
        let value = header.value.as_ref().ok_or_else(|| {
            WalRecordDecodeError::MissingNativeHeaderValue {
                name: header.key.clone(),
            }
        })?;
        let value = String::from_utf8(value.clone()).map_err(|_| {
            WalRecordDecodeError::InvalidNativeHeaderUtf8 {
                name: header.key.clone(),
            }
        })?;
        let name = name.to_string();
        if values.insert(name.clone(), value).is_some() {
            return Err(duplicate_error(name));
        }
    }
    Ok(values)
}

#[derive(Debug, Error)]
pub enum WalSinkError {
    #[error("wal sink append failed")]
    Append,
    #[error("wal record serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("wal producer failed: {0}")]
    Producer(#[from] ProducerError),
    #[error("wal producer delivery channel closed")]
    DeliveryCanceled,
}

#[derive(Debug, Error)]
pub enum IngestLimitError {
    #[error("ingest unauthorized for tenant `{tenant}`: {reason}")]
    Unauthorized { tenant: String, reason: String },
    #[error("ingest quota exceeded for tenant `{tenant}`: {reason}")]
    RateLimited { tenant: String, reason: String },
    #[error("ingest quota check unavailable for tenant `{tenant}`: {reason}")]
    Unavailable { tenant: String, reason: String },
}

#[derive(Debug, Error)]
pub enum QueryAuthorizationError {
    #[error("query unauthorized for tenant `{tenant}`: {reason}")]
    Unauthorized { tenant: String, reason: String },
    #[error("query authorization check unavailable for tenant `{tenant}`: {reason}")]
    Unavailable { tenant: String, reason: String },
}

#[derive(Debug, Error)]
pub enum WalConsumerError {
    #[error(transparent)]
    Consumer(#[from] ConsumerError),
    #[error("WAL consumer record {topic}-{partition}@{offset} did not include a value")]
    MissingValue {
        topic: String,
        partition: i32,
        offset: i64,
    },
}

#[derive(Debug, Error)]
pub enum HotTailPollError {
    #[error(transparent)]
    Consumer(#[from] WalConsumerError),
    #[error(transparent)]
    Decode(#[from] WalRecordDecodeError),
}

#[derive(Debug, Error)]
pub enum WalRecordDecodeError {
    #[error("wal record deserialization failed: {0}")]
    Deserialize(#[from] serde_json::Error),
    #[error("native Kafka log record is missing header {name}")]
    MissingNativeHeader { name: String },
    #[error("native Kafka log record header {name} has no value")]
    MissingNativeHeaderValue { name: String },
    #[error("native Kafka log record header {name} is not UTF-8")]
    InvalidNativeHeaderUtf8 { name: String },
    #[error("native Kafka log record timestamp `{value}` is invalid: {source}")]
    InvalidNativeTimestamp {
        value: String,
        source: std::num::ParseIntError,
    },
    #[error("invalid native Kafka timestamp `{value}`")]
    InvalidNativeTimestampValue { value: String },
    #[error("native Kafka log record value is not UTF-8")]
    InvalidNativeLogLineUtf8,
    #[error("native Kafka log record did not include any krabka-log-label-* headers")]
    MissingNativeLabels,
    #[error("invalid native Kafka label name {name}")]
    InvalidNativeLabelName { name: String },
    #[error("invalid native Kafka metadata name {name}")]
    InvalidNativeMetadataName { name: String },
    #[error("duplicate native Kafka label name {name}")]
    DuplicateNativeLabelName { name: String },
    #[error("duplicate native Kafka metadata name {name}")]
    DuplicateNativeMetadataName { name: String },
}

