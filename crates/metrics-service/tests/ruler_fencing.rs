use std::{collections::BTreeMap, time::Duration};

use assert2::{assert, check};
use krabka_broker::{Broker, BrokerConfig};
use krabka_client_admin::{AdminClient, CreateTopicSpec};
use krabka_client_coordination::{BrokerTransport, LeaseConfig, MemberId, Role};
use krabka_client_producer::ProducerRecord;
use krabka_metrics_service::{RulerLeaseState, advance_ruler_lease};
use krabka_units::secs;

const OUTPUT_TOPIC: &str = "ruler-fencing-output";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn takeover_fences_the_old_ruler_output_producer() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let directory = tempfile::tempdir().expect("broker directory");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve broker port");
    let address = listener.local_addr().expect("reserved broker address");
    drop(listener);
    let mut broker_config = BrokerConfig::for_tests(directory.path().to_path_buf());
    broker_config.listen_addr = address;
    broker_config.advertised_listener = address.to_string();
    let broker = Broker::start(broker_config).await.expect("broker start");
    let bootstrap = broker.listen_addr().to_string();
    create_output_topic(&bootstrap).await;
    let first = transport(&bootstrap, "first").await;
    let second = transport(&bootstrap, "second").await;
    let role = Role::new("metrics-ruler-1").unwrap();
    let first_member = MemberId::new("first").unwrap();
    let second_member = MemberId::new("second").unwrap();
    let config = LeaseConfig::default();
    advance(&first, &role, &first_member, None, config, 0).await;
    let now_ms = current_millis() + 100;
    let first_token =
        active_token(advance(&first, &role, &first_member, None, config, now_ms).await);
    let stale_producer = first.bound_producer(&role).await.unwrap();
    advance(&second, &role, &second_member, None, config, now_ms).await;
    let second_token = active_token(
        advance(
            &second,
            &role,
            &second_member,
            None,
            config,
            now_ms + 30_000,
        )
        .await,
    );
    check!(second_token > first_token);

    let transaction = stale_producer.begin_transaction().await.unwrap();
    let acknowledgement = stale_producer
        .send(ProducerRecord {
            topic: OUTPUT_TOPIC.to_owned(),
            partition: Some(0),
            key: Some(b"tenant-a".to_vec().into()),
            value: Some(b"ALERTS".to_vec().into()),
            headers: Vec::new(),
            timestamp_ms: None,
        })
        .await;
    let delivery = tokio::time::timeout(Duration::from_secs(20), acknowledgement)
        .await
        .expect("stale produce deadline");
    check!(delivery.is_ok(), "the broker stages the uncommitted record");
    let commit = transaction.commit().await;
    assert!(
        commit.is_err(),
        "stale output transaction unexpectedly committed"
    );
}

fn current_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

async fn advance(
    transport: &BrokerTransport,
    role: &Role,
    member: &MemberId,
    token: Option<krabka_client_coordination::FencingToken>,
    config: LeaseConfig,
    now_ms: i64,
) -> RulerLeaseState {
    let mut last = None;
    for _ in 0..20 {
        match advance_ruler_lease(transport, role, member, token, config, now_ms).await {
            Ok(state) => return state,
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("lease advance failed: {}", last.unwrap());
}

fn active_token(state: RulerLeaseState) -> krabka_client_coordination::FencingToken {
    match state {
        RulerLeaseState::Active { token, .. } => token,
        RulerLeaseState::Standby { .. } => panic!("member did not acquire the ruler lease"),
    }
}

async fn transport(bootstrap: &str, client: &str) -> BrokerTransport {
    BrokerTransport::builder()
        .bootstrap(bootstrap)
        .client_id(client)
        .topic_partitions(1)
        .topic_replication(1)
        .build()
        .await
        .expect("coordination transport")
}

async fn create_output_topic(bootstrap: &str) {
    let mut admin = AdminClient::connect(&[bootstrap.to_owned()])
        .await
        .expect("admin connect");
    let outcomes = admin
        .create_topics(
            &[CreateTopicSpec {
                name: OUTPUT_TOPIC.to_owned(),
                partitions: 1,
                replicas: 1,
                configs: BTreeMap::default(),
            }],
            secs(10),
        )
        .await
        .expect("create output topic");
    assert!(outcomes.iter().all(|outcome| outcome.error.is_none()));
}
