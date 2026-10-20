// The topic the WAL group suites consume, created and filled the same way
// for each.

use assert2::{assert, check};
use bytes::Bytes;
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::owned::create_topics_request::{CreatableTopic, CreateTopicsRequest};

pub const PARTITIONS: i32 = 2;
pub const RECORDS_PER_PARTITION: i64 = 4;

pub async fn create_topic(client: &krabka_client_core::Client, name: &str) {
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

/// The producer `fill` writes with, and the topic it writes to.
#[derive(Clone, Copy)]
pub struct WalFillProducer<'a> {
    pub bootstrap: &'a str,
    pub topic: &'a str,
    pub client_id: &'a str,
}

// Writes the same record count to every partition, pinned by index so the
// assertions do not depend on the producer's partitioner.
pub async fn fill(producer: WalFillProducer<'_>) {
    let WalFillProducer {
        bootstrap,
        topic,
        client_id,
    } = producer;
    let producer = Producer::builder()
        .bootstrap(bootstrap.to_owned())
        .client_id(client_id)
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
