//! What a consumer group rebalance does to a block builder, against a real
//! broker.
//!
//! The block builder buffers decoded windows across polls and commits offsets
//! only after it writes a block. These fixtures do not register a revoke
//! listener to flush those buffers. These tests state the consequence as
//! behaviour: a second member joins cooperatively, a partition leaves the
//! first member, and the records the first member had already polled are read
//! a second time by the member that takes them over.
//!
//! The replay assertion covers a consumer whose buffered records have not
//! reached a durable block before revocation.

mod support;
mod wal_group;

use std::time::{Duration, Instant};

use assert2::{assert, check};
use krabka_client_consumer::{Assignor, AutoOffsetReset, Consumer, ConsumerRecord};
use krabka_observability::wal_consumer_metrics::WalConsumerMetrics;
use krabka_traces::blockbuilder::{BlockBuilderConsumer, WalConsumerPoll as _};
use krabka_units::millis;

use self::wal_group::{PARTITIONS, RECORDS_PER_PARTITION, WalFillProducer, create_topic, fill};

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
    fill(WalFillProducer {
        bootstrap: &proc.bootstrap,
        topic,
        client_id: "krabka-traces-rebalance-producer",
    })
    .await;

    let group = ConsumerGroup {
        bootstrap: &proc.bootstrap,
        topic,
        group_id: "rebalance-reported",
    };
    let metrics = WalConsumerMetrics::unregistered();
    let mut first = BlockBuilderConsumer::new(group.member("first").await, &metrics);

    // The first member is alone, so it owns every partition. These are the
    // records a block builder would now hold in its accumulator.
    let held = drain(&mut first, per_partition() * 2).await;
    check!(held.len() == per_partition() * 2);
    for partition in 0..PARTITIONS {
        check!(metrics.partition_owned(topic, partition) == 1);
        check!(metrics.partition_revocations(topic, partition) == 0);
    }

    // A second member joins the same group. This fixture has no revoke listener
    // to flush the first member's buffered records.
    let mut second = join_second(&mut first, &group).await;
    let rebalance = poll_until_revoked(
        &mut GroupMembers {
            first: &mut first,
            second: &mut second,
        },
        RevocationWatch {
            metrics: &metrics,
            topic,
        },
    )
    .await;

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
    fill(WalFillProducer {
        bootstrap: &proc.bootstrap,
        topic,
        client_id: "krabka-traces-rebalance-producer",
    })
    .await;

    let group = ConsumerGroup {
        bootstrap: &proc.bootstrap,
        topic,
        group_id: "rebalance-replay",
    };
    let metrics = WalConsumerMetrics::unregistered();
    let mut first = BlockBuilderConsumer::new(group.member("first").await, &metrics);

    // The first member polls everything and commits nothing, exactly as a block
    // builder does between two flushes.
    let held = drain(&mut first, per_partition() * 2).await;
    check!(held.len() == per_partition() * 2);

    let mut second = join_second(&mut first, &group).await;
    let mut members = GroupMembers {
        first: &mut first,
        second: &mut second,
    };
    let rebalance = poll_until_revoked(
        &mut members,
        RevocationWatch {
            metrics: &metrics,
            topic,
        },
    )
    .await;
    assert!(let Some(&lost) = rebalance.revoked.first());

    // The member that took the partition over reads it from the last COMMITTED
    // offset, and no commit stands behind what the first member polled. So the
    // group reads those records a second time. The first member still holds
    // them, and its next flush writes them into a block keyed by its own
    // buffered offset range. That is the overlapping block the block store
    // cannot collapse: the two members pick different flush boundaries, so the
    // two keys differ.
    let replayed = drain_partition(
        &mut members,
        rebalance.polled_by_second,
        PartitionDrain {
            partition: lost,
            expected: per_partition(),
        },
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

/// One consumer group on one topic of the test broker.
struct ConsumerGroup<'a> {
    bootstrap: &'a str,
    topic: &'a str,
    group_id: &'a str,
}

impl ConsumerGroup<'_> {
    /// Joins the group as the member named `client`.
    async fn member(&self, client: &str) -> Consumer {
        let Self {
            bootstrap,
            topic,
            group_id,
        } = *self;
        Consumer::builder()
            .bootstrap(bootstrap.to_owned())
            .client_id(format!("krabka-traces-{group_id}-{client}"))
            .group_id(group_id.to_owned())
            .subscribe(vec![topic.to_owned()])
            .assignors(vec![Assignor::CooperativeSticky])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .expect("consumer build")
    }
}

/// Keeps the first member polling while the second member joins the group.
async fn join_second(first: &mut BlockBuilderConsumer, group: &ConsumerGroup<'_>) -> Consumer {
    let joining = group.member("second");
    tokio::pin!(joining);
    tokio::time::timeout(DEADLINE, async {
        loop {
            tokio::select! {
                consumer = &mut joining => return consumer,
                _ = first.poll(millis(100)) => {}
            }
        }
    })
    .await
    .expect("second member joins while first continues polling")
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

/// The two members of the group under test: the block builder's consumer
/// that owned every partition, and the plain consumer that joined after it.
struct GroupMembers<'a> {
    first: &'a mut BlockBuilderConsumer,
    second: &'a mut Consumer,
}

/// How many records of which partition [`drain_partition`] waits for.
#[derive(Clone, Copy)]
struct PartitionDrain {
    partition: i32,
    expected: usize,
}

/// Where [`poll_until_revoked`] watches for a revocation: the first member's
/// consumer metrics, for the WAL topic.
#[derive(Clone, Copy)]
struct RevocationWatch<'a> {
    metrics: &'a WalConsumerMetrics,
    topic: &'a str,
}

/// Returns the offsets of `expected` records of `partition`, in the order the
/// broker served them.
///
/// `already_polled` carries what the drive loop in [`poll_until_revoked`]
/// already took off the consumer. Those records are part of the answer, so this
/// starts from them rather than polling for records that have already arrived.
/// The first member keeps polling to finish the cooperative rebalance rounds.
async fn drain_partition(
    members: &mut GroupMembers<'_>,
    already_polled: Vec<ConsumerRecord>,
    drain: PartitionDrain,
) -> Vec<i64> {
    let PartitionDrain {
        partition,
        expected,
    } = drain;
    let deadline = Instant::now() + DEADLINE;
    let mut out: Vec<i64> = already_polled
        .into_iter()
        .filter(|record| record.partition == partition)
        .map(|record| record.offset)
        .collect();
    while out.len() < expected && Instant::now() < deadline {
        let _ = members.first.poll(millis(100)).await;
        assert!(let Ok(polled) = members.second.poll(millis(250)).await);
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
    members: &mut GroupMembers<'_>,
    watch: RevocationWatch<'_>,
) -> Rebalance {
    let RevocationWatch { metrics, topic } = watch;
    let deadline = Instant::now() + DEADLINE;
    let mut polled_by_second = Vec::new();
    while Instant::now() < deadline {
        // A poll that races the rebalance round can fail, and that failure is
        // not what these tests are about. The drive loop keeps going; `drain`
        // and `drain_partition` still hold their polls to `Ok`.
        if let Ok(polled) = members.second.poll(millis(100)).await {
            polled_by_second.extend(polled);
        }
        let _ = members.first.poll(millis(100)).await;
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
