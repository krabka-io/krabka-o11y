use std::{fmt, sync::Arc};

use async_trait::async_trait;
use krabka_client_producer::{Acks, Producer, ProducerError};
use krabka_ids::PartitionIndex;
use krabka_units::prelude::{Time, TimeExt as _, secs};

use super::{AuditError, AuditRecord, AuditSink, audit_producer_record};

/// The default time that one audit record may wait for the broker's ack.
pub const DEFAULT_AUDIT_WRITE_TIMEOUT: Time = secs(10);

/// An [`AuditSink`] that writes each audit record to one partition of a Kafka
/// topic through `krabka-client-producer`.
///
/// # One partition
///
/// Each record carries a `seq` header and a `prev_hash` header, and the
/// verifier reads a partition in offset order to walk that chain. Kafka keeps
/// the order of records only inside one partition. So the sink writes every
/// record to one partition, and one writer owns that partition. The broker's
/// own audit sink does the same.
///
/// The audit writer waits for each write before it gives the sink the next
/// record, so no two records from this sink are in flight together.
///
/// # Failure
///
/// A producer error, a canceled delivery, and a missing ack after the write
/// timeout all return [`AuditError::Sink`]. The writer then puts the record in
/// its spool and writes it again later. A record whose ack arrived after the
/// timeout can therefore reach the topic two times. The verifier reports that
/// as a break in the chain, and no record is lost.
pub struct KafkaTopicAuditSink {
    producer: Arc<Producer>,
    topic: String,
    partition: PartitionIndex,
    write_timeout: Time,
}

impl KafkaTopicAuditSink {
    /// A sink that writes to `partition` of `topic` through `producer`.
    #[must_use]
    pub fn new(
        producer: Producer,
        topic: impl Into<String>,
        partition: PartitionIndex,
        write_timeout: Time,
    ) -> Self {
        Self {
            producer: Arc::new(producer),
            topic: topic.into(),
            partition,
            write_timeout,
        }
    }

    /// Starts a producer at `bootstrap` and gives a sink over it, with the
    /// default write timeout.
    ///
    /// The producer waits for every in-sync replica to acknowledge a record.
    ///
    /// # Errors
    ///
    /// Returns the producer's error when the producer does not start.
    pub async fn connect(
        bootstrap: impl Into<String>,
        client_id: impl Into<String>,
        topic: impl Into<String>,
        partition: PartitionIndex,
        security: Option<&krabka_client_core::ClientSecurity>,
    ) -> Result<Self, ProducerError> {
        let producer = Producer::builder()
            .bootstrap(bootstrap)
            .client_id(client_id)
            .maybe_security(security.cloned())
            .acks(Acks::All)
            .build()
            .await?;
        Ok(Self::new(
            producer,
            topic,
            partition,
            DEFAULT_AUDIT_WRITE_TIMEOUT,
        ))
    }
}

#[async_trait]
impl AuditSink for KafkaTopicAuditSink {
    async fn write(&self, record: AuditRecord) -> Result<(), AuditError> {
        let produce = async {
            let delivery = self
                .producer
                .send(audit_producer_record(&self.topic, self.partition, record))
                .await;
            delivery.await
        };
        match tokio::time::timeout(self.write_timeout.to_std(), produce).await {
            Ok(Ok(Ok(_metadata))) => Ok(()),
            Ok(Ok(Err(error))) => Err(AuditError::Sink(format!(
                "audit topic {} partition {}: {error}",
                self.topic, self.partition
            ))),
            Ok(Err(_canceled)) => Err(AuditError::Sink(format!(
                "audit topic {} partition {}: the producer canceled the delivery",
                self.topic, self.partition
            ))),
            Err(_elapsed) => Err(AuditError::Sink(format!(
                "audit topic {} partition {}: no ack within {:?}",
                self.topic,
                self.partition,
                self.write_timeout.to_std()
            ))),
        }
    }
}

impl fmt::Debug for KafkaTopicAuditSink {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaTopicAuditSink")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .finish_non_exhaustive()
    }
}
