//! An in-process broker that holds the logs WAL topic, for the suites that
//! drive the WAL through a real broker.

use std::{collections::BTreeMap, sync::Arc};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle, authorizer::SimpleAclAuthorizer};
use krabka_client_admin::{
    AclEntry, AclOperation, AdminClient, CreateTopicSpec, PatternType, PermissionType, ResourceType,
};
use krabka_observability::ServiceConfig;
use krabka_units::secs;

pub struct WalBroker {
    /// Dropping it stops the broker.
    pub handle: BrokerHandle,
    /// Dropping it removes the broker's log directory.
    pub dir: tempfile::TempDir,
    pub bootstrap: String,
    pub wal_topic: String,
}

/// Starts a broker with an ACL authorizer, creates the default WAL topic on
/// it, and grants `tenant` every operation on that topic.
pub async fn start_wal_broker(tenant: &str) -> WalBroker {
    let dir = tempfile::tempdir().expect("broker tempdir");
    let mut broker_config = BrokerConfig::for_tests(dir.path().to_path_buf());
    broker_config.authorizer = Arc::new(SimpleAclAuthorizer::new(
        std::iter::once("ANONYMOUS".to_owned()).collect(),
    ));
    let handle = Broker::start(broker_config).await.expect("broker start");
    let bootstrap = handle.listen_addr().to_string();
    let wal_topic = ServiceConfig::default().wal_topic;
    let broker = WalBroker {
        handle,
        dir,
        bootstrap,
        wal_topic,
    };
    broker.create_wal_topic().await;
    broker.grant_tenant_wal_access(tenant).await;
    broker
}

impl WalBroker {
    /// Grants `tenant` every operation on the WAL topic.
    ///
    /// The pinned in-process broker runs an authorizer and answers `DescribeAcls`
    /// with the ACLs it holds, so the logs path reads its ACLs as configured. With
    /// no ACL at all it would refuse every tenant, as Kafka's authorizer does. A
    /// broker that answers `SECURITY_DISABLED` instead allows every tenant.
    async fn grant_tenant_wal_access(&self, tenant: &str) {
        let mut admin = AdminClient::connect(std::slice::from_ref(&self.bootstrap))
            .await
            .expect("admin connect");
        let outcomes = admin
            .create_acls(&[AclEntry {
                resource_type: ResourceType::Topic,
                resource_name: self.wal_topic.clone(),
                pattern_type: PatternType::Literal,
                principal: format!("User:{tenant}"),
                host: "*".to_string(),
                operation: AclOperation::All,
                permission_type: PermissionType::Allow,
            }])
            .await
            .expect("create the tenant's WAL topic ACL");
        assert!(
            outcomes.iter().all(|outcome| outcome.error.is_none()),
            "{outcomes:?}"
        );
    }

    async fn create_wal_topic(&self) {
        let mut admin = AdminClient::connect(std::slice::from_ref(&self.bootstrap))
            .await
            .expect("admin connect");
        admin
            .create_topics(
                &[CreateTopicSpec {
                    replica_assignments: BTreeMap::default(),
                    name: self.wal_topic.clone(),
                    partitions: 1,
                    replicas: 1,
                    configs: BTreeMap::default(),
                }],
                krabka_client_admin::TopicMutationOptions::with_timeout(secs(10)),
            )
            .await
            .expect("create the logs WAL topic");
    }
}
