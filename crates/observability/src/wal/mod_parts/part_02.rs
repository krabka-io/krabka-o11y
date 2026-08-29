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

