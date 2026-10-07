use assert2::assert;
use clap::Parser;
use krabka_broker::{Broker, BrokerConfig};
use krabka_metrics::wire::pb;
use krabka_observability::topic_contract::{METRICS_TOPICS, TopicSettings, provision_topics};
use prost::Message as _;

use super::*;

#[test]
fn all_requires_writer_options_and_rejects_divergent_backends() {
    assert!(Cli::try_parse_from(["service", "--target", "all"]).is_err());
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("writer.yaml");
    std::fs::write(&config, "listen: 127.0.0.1:8080\nbootstrap: broker:9092\n").unwrap();
    let parse = |extra: &[&str]| {
        let mut args = vec![
            "service",
            "--target",
            "all",
            "--writer-config",
            config.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        Cli::try_parse_from(args).unwrap()
    };
    let mut cli = parse(&[]);
    let writer = writer_config(&mut cli).unwrap();
    assert!(
        (cli.wal_bootstrap.as_deref(), writer.bootstrap()) == (Some("broker:9092"), "broker:9092")
    );
    for extra in [
        vec!["--listen", "0.0.0.0:8080"],
        vec!["--wal-bootstrap", "different:9092"],
        vec!["--wal-group-id", "krabka-metrics-block-builder"],
        vec!["--object-store-url", "file:///different"],
        vec!["--manifest-prefix", "different"],
        vec!["--wal-topic", "different"],
        vec!["--runtime-overrides", "/different.yaml"],
    ] {
        assert!(writer_config(&mut parse(&extra)).is_err(), "{extra:?}");
    }
    // The embedded writer parser refuses process settings instead of ignoring them.
    for setting in [
        "admin-listen-addr: 0.0.0.0:9404",
        "target: distributor",
        "audit-topic: audit",
        "wal-security-protocol: SSL",
    ] {
        std::fs::write(&config, format!("listen: 127.0.0.1:8080\n{setting}\n")).unwrap();
        assert!(writer_config(&mut parse(&[])).is_err(), "{setting}");
    }
}

#[tokio::test]
async fn one_registry_reports_independent_role_instruments_without_duplicate_names() {
    let registry = Arc::new(Mutex::new(Registry::default()));
    for role in [
        RoleKind::Distributor,
        RoleKind::BlockBuilder,
        RoleKind::Compactor,
    ] {
        let metrics =
            krabka_metrics::metrics::ServiceMetrics::for_role(Arc::clone(&registry), role).await;
        metrics.blocks_compacted.inc_by(3);
    }
    let query =
        krabka_promql::metrics::ServiceMetrics::for_role(Arc::clone(&registry), RoleKind::Querier)
            .await;
    query.query_started();
    let mut encoded = String::new();
    prometheus_client::encoding::text::encode(&mut encoded, &*registry.lock().await).unwrap();
    let names = encoded
        .lines()
        .filter_map(|line| {
            line.strip_prefix("# HELP ")
                .and_then(|line| line.split_whitespace().next())
        })
        .collect::<Vec<_>>();
    assert!(
        names
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == names.len()
    );
    let actual = encoded
        .lines()
        .filter(|line| {
            !line.starts_with('#')
                && (line.contains("blocks_compacted_total ") || line.contains("active_queries "))
        })
        .collect::<Vec<_>>();
    assert!(
        actual
            == vec![
                "krabka_metrics_distributor_blocks_compacted_total 3",
                "krabka_metrics_block_builder_blocks_compacted_total 3",
                "krabka_metrics_compactor_blocks_compacted_total 3",
                "krabka_metrics_querier_active_queries 1",
            ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_drains_acknowledged_writes_and_a_fresh_cold_querier_reads_the_whole_ledger() {
    let directory = tempfile::tempdir().unwrap();
    let broker = Broker::start(BrokerConfig::for_tests(directory.path().join("broker")))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    provision_topics(
        &bootstrap,
        &METRICS_TOPICS,
        &TopicSettings::single_broker(),
        None,
    )
    .await
    .unwrap();
    let store = directory.path().join("blocks");
    std::fs::create_dir(&store).unwrap();
    let url = url::Url::from_directory_path(&store).unwrap();
    let mut ports = Vec::new();
    for _ in 0..3 {
        ports.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let addresses = ports
        .iter()
        .map(|port| port.local_addr().unwrap())
        .collect::<Vec<_>>();
    drop(ports);
    let writer_path = directory.path().join("writer.yaml");
    std::fs::write(&writer_path, format!(
        "listen: {}\nbootstrap: {bootstrap}\nobject-store-url: {url}\nblock-builder-flush-max-age: 24h\nblock-builder-flush-max-rows: 100000\nblock-builder-poll-timeout: 10ms\ncompactor-interval: 24h\n",
        addresses[0],
    )).unwrap();
    let cli = Cli::try_parse_from([
        "service",
        "--target",
        "all",
        "--writer-config",
        writer_path.to_str().unwrap(),
        "--listen",
        &addresses[1].to_string(),
        "--admin-listen-addr",
        &addresses[2].to_string(),
        "--object-store-url",
        url.as_str(),
        "--wal-poll-timeout",
        "10ms",
    ])
    .unwrap();
    let stopping = CancellationToken::new();
    let role = tokio::spawn(serve_all(
        cli,
        ServerSecurity::default(),
        None,
        stopping.clone(),
    ));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(60), async {
        loop {
            if let Ok(response) = client
                .get(format!("http://{}/ready", addresses[2]))
                .send()
                .await
                && response.status().is_success()
            {
                break;
            }
            assert!(!role.is_finished(), "all stopped during startup");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("all roles reach their real readiness gates");
    let recovery: serde_json::Value = client
        .get(format!("http://{}/status/recovery", addresses[2]))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let consumers = recovery["wal_consumers"].as_array().unwrap();
    assert!(
        consumers.len() == 2
            && consumers
                .iter()
                .all(|consumer| consumer["caught_up"] == true)
    );
    assert!(
        consumers[0]["partitions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|partition| partition["topic"] == WAL_TOPIC && partition["assigned"] == true)
    );
    let expected = (0..1001).map(|id| {
        serde_json::json!({"metric":{"__name__":"shutdown_ledger","series":id.to_string()},"value":[1000,id.to_string()]})
    }).collect::<Vec<_>>();
    let write = pb::v1::WriteRequest {
        timeseries: (0..1001)
            .map(|id| pb::v1::TimeSeries {
                labels: vec![
                    pb::v1::Label {
                        name: "__name__".into(),
                        value: "shutdown_ledger".into(),
                    },
                    pb::v1::Label {
                        name: "series".into(),
                        value: id.to_string(),
                    },
                ],
                samples: vec![pb::v1::Sample {
                    timestamp: 1_000_000,
                    value: f64::from(id),
                }],
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let response = client
        .post(format!("http://{}/api/v1/push", addresses[0]))
        .header("Content-Type", "application/x-protobuf")
        .header("Content-Encoding", "snappy")
        .header("X-Scope-OrgID", "tenant")
        .body(
            snap::raw::Encoder::new()
                .compress_vec(&write.encode_to_vec())
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(response.status() == reqwest::StatusCode::OK);
    stopping.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(60), role)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    for address in &addresses {
        assert!(tokio::net::TcpStream::connect(address).await.is_err());
    }
    // A new querier has no WAL head. Every result must come from the durable blocks.
    let cli = Cli::try_parse_from([
        "service",
        "--target",
        "querier",
        "--object-store-url",
        url.as_str(),
        "--listen",
        &addresses[1].to_string(),
    ])
    .unwrap();
    let readiness = RoleReadiness::new();
    let stop = Shutdown::new();
    let querier = tokio::spawn(run_querier(
        cli,
        krabka_promql::metrics::ServiceMetrics::new(),
        readiness.clone(),
        ServerSecurity::default(),
        None,
        super::super::AuditHandle::disabled(),
        stop.clone(),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while !readiness.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let body: serde_json::Value = client
        .get(format!(
            "http://{}/api/v1/query?query=shutdown_ledger&time=1000",
            addresses[1]
        ))
        .header("X-Scope-OrgID", "tenant")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let mut actual = body["data"]["result"].as_array().unwrap().clone();
    let mut expected = expected;
    sort_ledger(&mut actual);
    sort_ledger(&mut expected);
    assert!(
        serde_json::json!({"resultType":"vector","result":actual})
            == serde_json::json!({"resultType":"vector","result":expected})
    );
    stop.trigger();
    querier.await.unwrap().unwrap();
    broker.shutdown().await;
}

fn sort_ledger(values: &mut [serde_json::Value]) {
    values.sort_by_key(|value| {
        value["metric"]["series"]
            .as_str()
            .unwrap()
            .parse::<usize>()
            .unwrap()
    });
}
