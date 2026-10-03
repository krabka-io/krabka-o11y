//! What scaling a block builder from one replica to two does to the member
//! that was already running, against a real broker.
//!
//! The Kubernetes qualification scales each block builder to two replicas
//! while no producer writes to the WAL. The first replica then goes through a
//! rebalance with an idle log. These tests drive that rebalance with the
//! consumer settings and the rebalance listener that the roles use, and state
//! two results:
//!
//! - both members report WAL catch-up again, so the first replica does not
//!   stay unready after the rebalance closes its catch-up gate, and
//! - the records that the first member fenced at the rebalance are written
//!   again, so its next commit does not move past them.

mod support;

use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_observability::{
    ReadinessGate, RoleReadiness, wal_consumer_metrics::WalConsumerMetrics,
    wal_group_assignment::WalRebalanceListener,
};
use krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use krabka_traces::blockbuilder::{
    BlockBuilderConsumer, WalConsumerCommit as _, WalConsumerPoll as _,
};
use krabka_units::millis;

const PARTITIONS: i32 = 2;
const RECORDS_PER_PARTITION: i64 = 4;
const DEADLINE: Duration = Duration::from_mins(1);

/// One member's view of the group: the consumer the role drives, its
/// catch-up gate, and what it holds and has written.
struct Member {
    consumer: BlockBuilderConsumer,
    gate: ReadinessGate,
    buffer: Vec<(i32, i64)>,
    written: Vec<(i32, i64)>,
}

impl Member {
    /// Joins `group` with the settings of the block builder roles. They keep
    /// the client's default assignor list, so the group rebalances eagerly.
    async fn join(bootstrap: &str, topic: &str, group: &str) -> Self {
        let rebalance = WalRebalanceListener::new(topic).rewinding_fenced_partitions();
        let consumer = Consumer::builder()
            .bootstrap(bootstrap.to_owned())
            .group_id(group.to_owned())
            .subscribe(vec![topic.to_owned()])
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .isolation_level(IsolationLevel::ReadCommitted)
            .rebalance_listener(Box::new(rebalance.clone()))
            .enable_auto_commit(false)
            .build()
            .await
            .expect("consumer build");
        let gate = RoleReadiness::new().gate("wal-catch-up");
        Self {
            consumer: BlockBuilderConsumer::with_catch_up(
                consumer,
                &WalConsumerMetrics::unregistered(),
                gate.clone(),
                rebalance,
            ),
            gate,
            buffer: Vec::new(),
            written: Vec::new(),
        }
    }

    /// One poll of the block builder loop: fence the revoked partitions,
    /// buffer the new records, and optionally flush and commit the buffer.
    async fn step(&mut self, flush: bool) {
        let polled = self.consumer.poll(millis(100)).await.unwrap_or_default();
        let revoked = self.consumer.take_revoked_partitions();
        self.buffer
            .retain(|(partition, _)| !revoked.contains(partition));
        self.buffer.extend(
            polled
                .iter()
                .map(|record| (record.partition, record.offset)),
        );
        if flush && !self.buffer.is_empty() {
            assert!(let Ok(()) = self.consumer.commit_sync().await);
            self.written.append(&mut self.buffer);
        }
    }
}

/// What the scale-out left behind.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    first_ready: bool,
    second_ready: bool,
    /// Every record that some member wrote, without duplicates.
    written: BTreeSet<(i32, i64)>,
}

/// Starts one member, lets it read the whole WAL, then scales the group to
/// two members with no new writes. `flushed_before_join` says whether the
/// first member had written its buffer before the second member joined.
async fn scale_out(name: &str, flushed_before_join: bool) -> Outcome {
    let proc = support::start().await;
    let topic = format!("__traces_scale_out_{name}");
    create_topic(&proc.client, &topic).await;
    fill(&proc.bootstrap, &topic).await;

    let mut first = Member::join(&proc.bootstrap, &topic, name).await;
    let deadline = Instant::now() + DEADLINE;
    while first.buffer.len() < all_records() && Instant::now() < deadline {
        first.step(false).await;
    }
    check!(first.buffer.len() == all_records());
    if flushed_before_join {
        first.step(true).await;
    }

    // The second member joins on its own task, as a second pod does. The first
    // member keeps polling, and no poll of it is cancelled part way.
    let joining = tokio::spawn({
        let (bootstrap, topic, name) = (proc.bootstrap.clone(), topic.clone(), name.to_owned());
        async move { Member::join(&bootstrap, &topic, &name).await }
    });
    let deadline = Instant::now() + DEADLINE;
    while !joining.is_finished() && Instant::now() < deadline {
        first.step(false).await;
    }
    assert!(let Ok(mut second) = joining.await);

    // Poll until the group reaches the expected outcome, or until the
    // deadline. A healthy run stops after a few rounds; a broken one shows its
    // last state at the deadline.
    let deadline = Instant::now() + DEADLINE;
    loop {
        first.step(true).await;
        second.step(true).await;
        let outcome = Outcome {
            first_ready: first.gate.is_ready(),
            second_ready: second.gate.is_ready(),
            written: first
                .written
                .iter()
                .chain(&second.written)
                .copied()
                .collect(),
        };
        if outcome == expected() || Instant::now() >= deadline {
            return outcome;
        }
    }
}

/// Both members caught up, and every record written.
fn expected() -> Outcome {
    Outcome {
        first_ready: true,
        second_ready: true,
        written: (0..PARTITIONS)
            .flat_map(|partition| (0..RECORDS_PER_PARTITION).map(move |offset| (partition, offset)))
            .collect(),
    }
}

#[tokio::test]
async fn both_members_are_caught_up_and_every_record_is_written_after_a_scale_out() {
    let cases = [("flushed", true), ("buffered", false)];
    for (name, flushed_before_join) in cases {
        let outcome = Box::pin(scale_out(name, flushed_before_join)).await;
        check!(outcome == expected(), "case {name}");
    }
}

fn all_records() -> usize {
    usize::try_from(i64::from(PARTITIONS) * RECORDS_PER_PARTITION)
        .expect("the record count fits a usize")
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
        .client_id("krabka-traces-scale-out-producer")
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
            assert!(let Ok(_) = ack);
        }
    }
}
