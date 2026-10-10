//! The profiles WAL topic on a test broker.
//!
//! Shared by this binary's suites and, through `#[path]`, by suites under
//! `tests/`, so it names only external crates.

use std::collections::BTreeMap;

use krabka_client_admin::{AdminClient, CreateTopicSpec, TopicMutationOptions};
use krabka_profiles::PROFILES_WAL_TOPIC;

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
