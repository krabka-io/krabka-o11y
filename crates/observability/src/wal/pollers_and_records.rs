use krabka_units::convert::TimeExt;

use crate::{
    Arc, AtomicOrdering, BTreeMap, BrokerBackedQueryAuthorizer, BufferedLogHotTail,
    CancellationToken, ClientResourcePolicy, ConsumerError, DeferredWalConsumerConnect, Error,
    JoinHandle, KafkaLogWalConsumer, KafkaWalHeader, KafkaWalRecord, LogQueryAuthorizer,
    ProducerError, ServiceReadiness, SharedCompactionFrontier, Time, WalLogRecord, WalPosition,
    is_loki_label_name, poll_log_hot_tail_once_with_frontier, sleep,
};
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
pub(crate) fn spawn_wal_hot_tail_connect_and_poll(
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
pub(crate) fn spawn_query_authorizer_connect(
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

pub(crate) fn has_native_kafka_log_headers(headers: &[KafkaWalHeader]) -> bool {
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

pub(crate) fn decode_native_kafka_log_record(
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

pub(crate) fn required_kafka_header_utf8(
    headers: &[KafkaWalHeader],
    name: &str,
) -> Result<String, WalRecordDecodeError> {
    optional_kafka_header_utf8(headers, name)?.ok_or_else(|| {
        WalRecordDecodeError::MissingNativeHeader {
            name: name.to_string(),
        }
    })
}

pub(crate) fn optional_kafka_header_utf8(
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

pub(crate) fn native_timestamp_ms_to_ns(timestamp_ms: i64) -> Result<i64, WalRecordDecodeError> {
    let converted_ns = timestamp_ms.checked_mul(1_000_000).ok_or_else(|| {
        WalRecordDecodeError::InvalidNativeTimestampValue {
            value: timestamp_ms.to_string(),
        }
    })?;
    validate_native_timestamp_ns(converted_ns, timestamp_ms.to_string())
}

pub(crate) fn validate_native_timestamp_ns(
    timestamp_ns: i64,
    value: String,
) -> Result<i64, WalRecordDecodeError> {
    if timestamp_ns < 0 {
        Err(WalRecordDecodeError::InvalidNativeTimestampValue { value })
    } else {
        Ok(timestamp_ns)
    }
}

pub(crate) fn kafka_headers_with_prefix(
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
