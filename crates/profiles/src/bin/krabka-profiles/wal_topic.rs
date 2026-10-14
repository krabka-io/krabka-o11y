//! The profiles WAL topic on a test broker, and a broker that has it.
//!
//! Shared by this binary's suites and, through `#[path]`, by suites under
//! `tests/`, so it names only external crates.

use std::collections::BTreeMap;

use krabka_broker::BrokerHandle;
use krabka_client_admin::{AdminClient, CreateTopicSpec, TopicMutationOptions};
use krabka_profiles::PROFILES_WAL_TOPIC;

#[path = "../../../../integration/tests/support/test_broker.rs"]
mod test_broker;

/// An in-process broker, in a directory of its own, with the profiles WAL
/// topic already created.
pub struct WalTopicBroker {
    // Declared before the directory, so the broker stops before its files go.
    _broker: BrokerHandle,
    _tempdir: tempfile::TempDir,
    pub bootstrap: String,
}

impl WalTopicBroker {
    pub async fn start() -> Self {
        let started_broker = test_broker::start_broker().await;
        let bootstrap = started_broker.bootstrap;
        create_wal_topic(&bootstrap).await;
        Self {
            _broker: started_broker.broker,
            _tempdir: started_broker.tempdir,
            bootstrap,
        }
    }
}

/// Creates the profiles WAL topic, with one partition, on the broker at
/// `bootstrap`.
pub async fn create_wal_topic(bootstrap: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_string()])
        .await
        .expect("admin connect");
    admin
        .create_topics(
            &[CreateTopicSpec {
                replica_assignments: BTreeMap::default(),
                name: PROFILES_WAL_TOPIC.into(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            TopicMutationOptions::with_timeout(krabka_units::secs(5)),
        )
        .await
        .expect("create the profiles WAL topic");
}
