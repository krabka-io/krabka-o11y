use std::collections::BTreeMap;

use async_trait::async_trait;
use krabka_blockstore::{BrokerSnapshot, BrokerState, GroupOffset, WalOffset};
use krabka_client_admin::{AdminClient, IsolationLevel, OffsetSpec, groups::ListGroupsOptions};
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel as ConsumerIsolation};
use krabka_client_core::ClientSecurity;
use krabka_units::{Time, convert::TimeExt as _, millis, secs};

/// How long [`KafkaBrokerState::records_between`] reads before it gives up.
const RECORD_COUNT_DEADLINE: Time = secs(30);

/// Reads a [`BrokerSnapshot`] over the Kafka admin protocol.
///
/// The snapshot holds the high watermark of every partition of `topics`, and
/// the committed offset of every consumer group on those partitions. A group
/// with no committed offset on `topics` is not in the snapshot.
#[derive(Clone, Debug)]
pub struct KafkaBrokerState {
    bootstrap: String,
    security: Option<ClientSecurity>,
    topics: Vec<String>,
}

impl KafkaBrokerState {
    /// A reader of `topics` on the broker at `bootstrap`. `None` connects in
    /// plain text.
    #[must_use]
    pub fn new(
        bootstrap: impl Into<String>,
        security: Option<ClientSecurity>,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let mut topics = topics.into_iter().map(Into::into).collect::<Vec<_>>();
        topics.sort();
        topics.dedup();
        Self {
            bootstrap: bootstrap.into(),
            security,
            topics,
        }
    }

    async fn read(&self) -> Result<BrokerSnapshot, String> {
        let mut admin = AdminClient::connect_secured(
            std::slice::from_ref(&self.bootstrap),
            self.security.clone(),
        )
        .await
        .map_err(|error| format!("connect to {}: {error}", self.bootstrap))?;
        let names = self.topics.iter().map(String::as_str).collect::<Vec<_>>();
        let metadata = admin
            .metadata(&names)
            .await
            .map_err(|error| format!("read topic metadata: {error}"))?;
        let mut specs = BTreeMap::new();
        for topic in &self.topics {
            let entry = metadata
                .topics
                .iter()
                .find(|entry| &entry.name == topic)
                .ok_or_else(|| format!("the broker does not report topic `{topic}`"))?;
            if let Some(error) = &entry.error {
                return Err(format!("topic `{topic}` is unreadable: {}", error.name));
            }
            for partition in 0..entry.partition_count {
                specs.insert((topic.clone(), partition), OffsetSpec::Latest);
            }
        }
        let mut wal_offsets = Vec::with_capacity(specs.len());
        for ((topic, partition), listed) in admin
            .list_offsets(&specs, IsolationLevel::ReadUncommitted)
            .await
        {
            let listed = listed.map_err(|error| {
                format!("list the end offset of {topic}:{partition}: {}", error.name)
            })?;
            wal_offsets.push(WalOffset {
                topic,
                partition,
                next_offset: listed.offset,
            });
        }

        let groups = admin
            .list_groups(&ListGroupsOptions::default())
            .await
            .map_err(|error| format!("list consumer groups: {error}"))?
            .all()
            .map_err(|error| format!("list consumer groups: {}", error.error.name))?;
        let mut group_offsets = Vec::new();
        for group in groups {
            let committed = admin
                .list_consumer_group_offsets(&group.group_id)
                .await
                .map_err(|error| format!("read offsets of group `{}`: {error}", group.group_id))?;
            for ((topic, partition), next_offset) in committed {
                // A negative offset is Kafka's "no committed offset".
                if next_offset >= 0 && self.topics.binary_search(&topic).is_ok() {
                    group_offsets.push(GroupOffset {
                        group: group.group_id.clone(),
                        topic,
                        partition,
                        next_offset,
                    });
                }
            }
        }
        Ok(BrokerSnapshot {
            wal_offsets,
            group_offsets,
        }
        .sorted())
    }
}

#[async_trait]
impl BrokerState for KafkaBrokerState {
    async fn capture(&self) -> Result<BrokerSnapshot, String> {
        self.read().await
    }

    /// Reads the partition from `offset` as a `read_committed` consumer with
    /// no group, and counts what it returns.
    async fn records_between(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        next_offset: i64,
    ) -> Result<u64, String> {
        let consumer = Consumer::builder()
            .bootstrap(self.bootstrap.clone())
            .maybe_security(self.security.clone())
            .isolation_level(ConsumerIsolation::ReadCommitted)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .map_err(|error| format!("connect a reader of {topic}:{partition}: {error}"))?;
        count_records(consumer, topic, partition, offset, next_offset).await
    }
}

async fn count_records(
    mut consumer: Consumer,
    topic: &str,
    partition: i32,
    offset: i64,
    next_offset: i64,
) -> Result<u64, String> {
    let key = (topic.to_string(), partition);
    let read = |error| format!("read {topic}:{partition}: {error}");
    consumer
        .assign(std::slice::from_ref(&key))
        .await
        .map_err(read)?;
    consumer
        .seek(topic, partition, offset)
        .await
        .map_err(read)?;
    let deadline = tokio::time::Instant::now() + RECORD_COUNT_DEADLINE.to_std();
    let mut records = 0_u64;
    loop {
        // The position passes trailing markers and aborted records, so it
        // reaches `next_offset` once the reader has seen every record.
        if consumer.position(topic, partition).await.map_err(read)? >= next_offset {
            return Ok(records);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "read {topic}:{partition}: the reader did not reach offset {next_offset}"
            ));
        }
        for record in consumer.poll(millis(200)).await.map_err(read)? {
            if record.topic == topic && record.partition == partition && record.offset < next_offset
            {
                records += 1;
            }
        }
    }
}
