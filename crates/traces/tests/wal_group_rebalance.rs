//! What a consumer group rebalance does to a block builder, against a real
//! broker.
//!
//! The block builder buffers decoded windows across polls and commits offsets
//! only after it writes a block. `krabka-client-consumer` exposes no rebalance
//! callback, so nothing in this process runs between the group's decision to
//! move a partition and the move itself. These tests state the consequence as
//! behaviour: a second member joins, partitions leave the first member, and the
//! records the first member had already polled are read a second time by the
//! member that takes them over.
//!
//! The suite is the executable form of the limitation that
//! `krabka_observability::wal_group_assignment` documents. If a future
//! `krabka-client-consumer` grows a revoke callback and the block builder
//! flushes from it, `the_group_reads_the_first_members_polled_records_again`
//! is the test that must change.

mod support;

use std::time::{Duration, Instant};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_observability::wal_consumer_metrics::WalConsumerMetrics;
use krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use krabka_traces::blockbuilder::{BlockBuilderConsumer, WalConsumerPoll as _};
use krabka_units::millis;

const PARTITIONS: i32 = 2;
const RECORDS_PER_PARTITION: i64 = 4;
const DEADLINE: Duration = Duration::from_mins(1);

/// The records this test writes to one partition, as a `usize` count.
fn per_partition() -> usize {
    usize::try_from(RECORDS_PER_PARTITION).expect("the record count fits a usize")
}

#[tokio::test]
async fn a_second_group_member_takes_partitions_and_the_watch_reports_it() {
    let proc = support::start().await;
    let topic = "__traces_rebalance_reported_wal";
    create_topic(&proc.client, topic).await;
    fill(&proc.bootstrap, topic).await;

    let metrics = WalConsumerMetrics::unregistered();
    let mut first = BlockBuilderConsumer::new(
        member(&proc.bootstrap, topic, "rebalance-reported", "first").await,
        &metrics,
    );

    // The first member is alone, so it owns every partition. These are the
    // records a block builder would now hold in its accumulator.
    let held = drain(&mut first, per_partition() * 2).await;
    check!(held.len() == per_partition() * 2);
    for partition in 0..PARTITIONS {
        check!(metrics.partition_owned(topic, partition) == 1);
        check!(metrics.partition_revocations(topic, partition) == 0);
    }

    // A second member joins the same group. Nothing asks the first member to
    // flush, and nothing in this process is told before the partitions move.
    let mut second = member(&proc.bootstrap, topic, "rebalance-reported", "second").await;
    let rebalance = poll_until_revoked(&mut first, &mut second, &metrics, topic).await;

    // One partition of the two moved, and the instruments say which.
    check!(rebalance.revoked.len() == 1);
    assert!(let Some(&lost) = rebalance.revoked.first());
    let kept = 1 - lost;
    check!(metrics.partition_owned(topic, lost) == 0);
    check!(metrics.partition_revocations(topic, lost) == 1);
    check!(metrics.partition_owned(topic, kept) == 1);
    check!(metrics.partition_revocations(topic, kept) == 0);

    assert!(let Ok(()) = second.close().await);
}

#[tokio::test]
async fn the_group_reads_the_first_members_polled_records_again() {
    let proc = support::start().await;
    let topic = "__traces_rebalance_replay_wal";
    create_topic(&proc.client, topic).await;
    fill(&proc.bootstrap, topic).await;

    let metrics = WalConsumerMetrics::unregistered();
    let mut first = BlockBuilderConsumer::new(
        member(&proc.bootstrap, topic, "rebalance-replay", "first").await,
        &metrics,
    );

    // The first member polls everything and commits nothing, exactly as a block
    // builder does between two flushes.
    let held = drain(&mut first, per_partition() * 2).await;
    check!(held.len() == per_partition() * 2);

    let mut second = member(&proc.bootstrap, topic, "rebalance-replay", "second").await;
    let mut rebalance = poll_until_revoked(&mut first, &mut second, &metrics, topic).await;
    assert!(let Some(&lost) = rebalance.revoked.first());

    // The member that took the partition over reads it from the last COMMITTED
    // offset, and no commit stands behind what the first member polled. So the
    // group reads those records a second time. The first member still holds
    // them, and its next flush writes them into a block keyed by its own
    // buffered offset range. That is the overlapping block the block store
    // cannot collapse: the two members pick different flush boundaries, so the
    // two keys differ.
    let replayed = drain_partition(
        &mut second,
        &mut rebalance.polled_by_second,
        lost,
        per_partition(),
    )
    .await;
    let held_on_lost: Vec<i64> = held
        .iter()
        .filter(|record| record.partition == lost)
        .map(|record| record.offset)
        .collect();

    check!(held_on_lost.len() == per_partition());
    check!(replayed == held_on_lost);

    assert!(let Ok(()) = second.close().await);
}

async fn create_topic(client: &krabka_client_core::Client, name: &str) {
    let resp = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: PARTITIONS,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    assert!(let Some(created) = resp.topics.first());
    check!(created.error_code == 0);
}

/// Writes the same record count to every partition, pinned by index so the
/// assertions do not depend on the producer's partitioner.
async fn fill(bootstrap: &str, topic: &str) {
    let producer = Producer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id("krabka-traces-rebalance-producer")
        .build()
        .await
        .expect("producer build");
    for partition in 0..PARTITIONS {
        for index in 0..RECORDS_PER_PARTITION {
            let ack = producer
                .send(ProducerRecord {
                    topic: topic.to_owned(),
                    partition: Some(partition),
                    value: Some(Bytes::from(format!("{partition}:{index}"))),
                    ..ProducerRecord::default()
                })
                .await;
            assert!(let Ok(Ok(_)) = ack.await);
        }
    }
}

async fn member(bootstrap: &str, topic: &str, group: &str, client: &str) -> Consumer {
    Consumer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id(format!("krabka-traces-{group}-{client}"))
        .group_id(group.to_owned())
        .subscribe(vec![topic.to_owned()])
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("consumer build")
}

/// Polls until `expected` records arrive, or fails at the deadline.
async fn drain(consumer: &mut BlockBuilderConsumer, expected: usize) -> Vec<ConsumerRecord> {
    let deadline = Instant::now() + DEADLINE;
    let mut out = Vec::new();
    while Instant::now() < deadline {
        assert!(let Ok(polled) = consumer.poll(millis(250)).await);
        out.extend(polled);
        if out.len() >= expected {
            return out;
        }
    }
    panic!("timed out after {} records, wanted {expected}", out.len());
}

/// Returns the offsets of `expected` records of `partition`, in the order the
/// broker served them.
///
/// `already_polled` carries what the drive loop in [`poll_until_revoked`]
/// already took off the consumer. Those records are part of the answer, so this
/// starts from them rather than polling for records that have already arrived.
async fn drain_partition(
    consumer: &mut Consumer,
    already_polled: &mut Vec<ConsumerRecord>,
    partition: i32,
    expected: usize,
) -> Vec<i64> {
    let deadline = Instant::now() + DEADLINE;
    let mut out: Vec<i64> = std::mem::take(already_polled)
        .into_iter()
        .filter(|record| record.partition == partition)
        .map(|record| record.offset)
        .collect();
    while out.len() < expected && Instant::now() < deadline {
        assert!(let Ok(polled) = consumer.poll(millis(250)).await);
        out.extend(
            polled
                .into_iter()
                .filter(|record| record.partition == partition)
                .map(|record| record.offset),
        );
    }
    if out.len() >= expected {
        return out;
    }
    panic!(
        "timed out after {} records of partition {partition}, wanted {expected}",
        out.len()
    );
}

/// What one rebalance did, as the drive loop observed it.
struct Rebalance {
    /// The partitions the group took away from the first member.
    revoked: Vec<i32>,
    /// The records the second member polled while the loop ran.
    polled_by_second: Vec<ConsumerRecord>,
}

/// Drives both members until the group takes a partition away from `first`.
///
/// Both members have to keep polling. The coordinator task rejoins in the
/// background, but the watch only sees the new assignment when `first` polls,
/// and the group only completes a round when `second` polls as well. The loop
/// keeps what `second` polls, because those records are the replay the second
/// test measures.
async fn poll_until_revoked(
    first: &mut BlockBuilderConsumer,
    second: &mut Consumer,
    metrics: &WalConsumerMetrics,
    topic: &str,
) -> Rebalance {
    let deadline = Instant::now() + DEADLINE;
    let mut polled_by_second = Vec::new();
    while Instant::now() < deadline {
        // A poll that races the rebalance round can fail, and that failure is
        // not what these tests are about. The drive loop keeps going; `drain`
        // and `drain_partition` still hold their polls to `Ok`.
        if let Ok(polled) = second.poll(millis(100)).await {
            polled_by_second.extend(polled);
        }
        let _ = first.poll(millis(100)).await;
        let revoked: Vec<i32> = (0..PARTITIONS)
            .filter(|partition| metrics.partition_revocations(topic, *partition) > 0)
            .collect();
        if !revoked.is_empty() {
            return Rebalance {
                revoked,
                polled_by_second,
            };
        }
    }
    panic!("the group never moved a partition off the first member");
}
