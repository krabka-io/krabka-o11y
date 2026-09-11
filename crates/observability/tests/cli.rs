use assert2::assert;
use clap::{Parser, ValueEnum as _};
use krabka_observability::{
    QuerierIndexSource, Role, ServiceConfig, build_service_dependencies, run,
};
use krabka_units::{bytes, kibibytes, millis, nanos};

#[test]
fn parses_explicit_service_targets() {
    for (target, expected) in [
        ("distributor", Role::Distributor),
        ("block-builder", Role::BlockBuilder),
        ("querier", Role::Querier),
        ("all", Role::All),
    ] {
        let config =
            ServiceConfig::try_parse_from(["krabka-observability", "--target", target]).unwrap();

        assert!(config.target == expected);
        assert!(run(config).unwrap().role == expected);
    }
}

/// One stage, one spelling. A `--target` value that drifted from the shared
/// vocabulary would put a second name on a stage an operator already runs
/// under another signal, which is exactly the hazard the vocabulary removes.
/// The check goes through clap rather than through the enum's `Debug`, because
/// the string clap accepts is the one a manifest carries.
#[test]
fn every_target_is_spelled_as_the_shared_vocabulary_spells_it() {
    for role in Role::value_variants() {
        let possible = role
            .to_possible_value()
            .expect("every role is a possible value");
        assert!(possible.get_name() == role.kind().as_str(), "{role:?}");
        assert!(
            ServiceConfig::try_parse_from([
                "krabka-observability",
                "--target",
                role.kind().as_str(),
            ])
            .expect("the shared name parses")
            .target
                == *role
        );
    }
}

#[test]
fn parses_unit_bearing_query_length() {
    let config = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target",
        "querier",
        "--max-query-string-bytes",
        "64B",
    ])
    .unwrap();

    assert!(config.max_query_string_bytes.is_some());
}

#[test]
fn rejects_negative_quantity_limits() {
    for (flag, value) in [
        ("--max-query-range", "-1ns"),
        ("--max-query-read", "-1B"),
        ("--max-query-string-bytes", "-1B"),
        ("--max-ingest-body", "-1B"),
        ("--wal-append-timeout", "-1ms"),
    ] {
        let argument = format!("{flag}={value}");
        assert!(
            ServiceConfig::try_parse_from([
                "krabka-observability",
                "--target",
                "querier",
                &argument,
            ])
            .is_err(),
            "{flag} accepted {value}"
        );
    }
}

#[test]
fn parses_querier_object_store_shard_catalog_config() {
    let config = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target",
        "querier",
        "--listen-addr",
        "127.0.0.1:3200",
        "--object-store-url",
        "s3://krabka-observability",
        "--data-root",
        "/var/lib/krabka-observability",
        "--querier-index-source",
        "tenant-object-store-shards",
        "--tenant",
        "tenant-a",
        "--index-prefix",
        "observability/logs",
        "--query-start-ns",
        "10",
        "--query-end-ns",
        "30",
        "--max-query-range",
        "20ns",
        "--max-query-series",
        "10",
        "--max-query-read",
        "1KiB",
        "--max-query-string-bytes",
        "64B",
    ])
    .unwrap();

    assert!(
        config
            == ServiceConfig {
                target: Role::Querier,
                listen_addr: "127.0.0.1:3200".parse().unwrap(),
                object_store_url: Some("s3://krabka-observability".to_string()),
                wal_bootstrap_server: None,
                wal_topic: "__krabka_observability_logs_wal".to_string(),
                wal_group_id: "krabka-observability-block-builder".to_string(),
                data_root: "/var/lib/krabka-observability".into(),
                querier_index_source: QuerierIndexSource::TenantObjectStoreShards,
                tenant: Some("tenant-a".to_string()),
                index_prefix: Some("observability/logs".to_string()),
                query_start_ns: Some(10),
                query_end_ns: Some(30),
                max_query_range: Some(nanos(20)),
                max_query_series: Some(10),
                max_query_read: Some(kibibytes(1)),
                max_query_string_bytes: Some(bytes(64)),
                max_ingest_body: None,
                wal_append_timeout: None,
                ..ServiceConfig::default()
            }
    );
}

#[test]
fn parses_distributor_wal_config() {
    let config = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target",
        "distributor",
        "--wal-bootstrap-server",
        "127.0.0.1:9092",
        "--wal-topic",
        "__krabka_observability_logs_wal",
        "--max-ingest-body",
        "2KiB",
        "--wal-append-timeout",
        "250ms",
    ])
    .unwrap();

    assert!(
        config
            == ServiceConfig {
                target: Role::Distributor,
                listen_addr: "0.0.0.0:3100".parse().unwrap(),
                object_store_url: None,
                wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                wal_topic: "__krabka_observability_logs_wal".to_string(),
                wal_group_id: "krabka-observability-block-builder".to_string(),
                data_root: ".".into(),
                querier_index_source: QuerierIndexSource::LocalManifest,
                tenant: None,
                index_prefix: None,
                query_start_ns: None,
                query_end_ns: None,
                max_query_range: None,
                max_query_series: None,
                max_query_read: None,
                max_query_string_bytes: None,
                max_ingest_body: Some(kibibytes(2)),
                wal_append_timeout: Some(millis(250)),
                ..ServiceConfig::default()
            }
    );
}

#[test]
fn parses_block_builder_wal_consumer_config() {
    let config = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target",
        "block-builder",
        "--wal-bootstrap-server",
        "127.0.0.1:9092",
        "--wal-topic",
        "__krabka_observability_logs_wal",
        "--wal-group-id",
        "krabka-observability-block-builder",
        "--object-store-url",
        "file:///tmp/krabka-observability",
        "--index-prefix",
        "observability/logs",
    ])
    .unwrap();

    assert!(
        config
            == ServiceConfig {
                target: Role::BlockBuilder,
                listen_addr: "0.0.0.0:3100".parse().unwrap(),
                object_store_url: Some("file:///tmp/krabka-observability".to_string()),
                wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                wal_topic: "__krabka_observability_logs_wal".to_string(),
                wal_group_id: "krabka-observability-block-builder".to_string(),
                data_root: ".".into(),
                querier_index_source: QuerierIndexSource::LocalManifest,
                tenant: None,
                index_prefix: Some("observability/logs".to_string()),
                query_start_ns: None,
                query_end_ns: None,
                max_query_range: None,
                max_query_series: None,
                max_query_read: None,
                max_query_string_bytes: None,
                max_ingest_body: None,
                wal_append_timeout: None,
                ..ServiceConfig::default()
            }
    );
}

#[test]
fn parses_querier_wal_tail_config() {
    let config = ServiceConfig::try_parse_from([
        "krabka-observability",
        "--target",
        "querier",
        "--wal-bootstrap-server",
        "127.0.0.1:9092",
        "--wal-topic",
        "__krabka_observability_logs_wal",
        "--wal-group-id",
        "krabka-observability-querier-tail",
    ])
    .unwrap();

    assert!(
        config
            == ServiceConfig {
                target: Role::Querier,
                listen_addr: "0.0.0.0:3100".parse().unwrap(),
                object_store_url: None,
                wal_bootstrap_server: Some("127.0.0.1:9092".to_string()),
                wal_topic: "__krabka_observability_logs_wal".to_string(),
                wal_group_id: "krabka-observability-querier-tail".to_string(),
                data_root: ".".into(),
                querier_index_source: QuerierIndexSource::LocalManifest,
                tenant: None,
                index_prefix: None,
                query_start_ns: None,
                query_end_ns: None,
                max_query_range: None,
                max_query_series: None,
                max_query_read: None,
                max_query_string_bytes: None,
                max_ingest_body: None,
                wal_append_timeout: None,
                ..ServiceConfig::default()
            }
    );
}

#[tokio::test]
async fn querier_dependencies_require_wal_bootstrap_server() {
    let config = ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:0".parse().unwrap(),
        object_store_url: None,
        wal_bootstrap_server: None,
        wal_topic: "__krabka_observability_logs_wal".to_string(),
        wal_group_id: "krabka-observability-querier-tail".to_string(),
        data_root: ".".into(),
        querier_index_source: QuerierIndexSource::LocalManifest,
        tenant: None,
        index_prefix: None,
        query_start_ns: None,
        query_end_ns: None,
        max_query_range: None,
        max_query_series: None,
        max_query_read: None,
        max_query_string_bytes: None,
        max_ingest_body: None,
        wal_append_timeout: None,
        ..ServiceConfig::default()
    };

    match build_service_dependencies(
        &config,
        krabka_observability::wal_consumer_metrics::WalConsumerMetrics::unregistered(),
    )
    .await
    {
        Ok(_) => panic!("querier dependencies should require WAL bootstrap config"),
        Err(error) => {
            assert!(error.to_string().contains("missing --wal-bootstrap-server"));
        }
    }
}

#[test]
fn rejects_missing_target() {
    let error = ServiceConfig::try_parse_from(["krabka-observability"]).unwrap_err();

    assert!(error.to_string().contains("--target"));
}

#[test]
fn rejects_unknown_target() {
    let error = ServiceConfig::try_parse_from(["krabka-observability", "--target", "ingester"])
        .unwrap_err();

    assert!(error.to_string().contains("invalid value"));
}

/// A service that binds loopback inside a container is unreachable from
/// outside the pod, and the only symptom is a health check that fails with
/// nothing in the logs. Loki, Mimir and Tempo all default their HTTP listener
/// to every interface; so does this.
#[test]
fn the_default_listen_address_is_reachable_from_outside_the_container() {
    let config =
        ServiceConfig::try_parse_from(["krabka-observability", "--target", "querier"]).unwrap();

    assert!(config.listen_addr.ip().is_unspecified());
    assert!(config.listen_addr.port() == 3100);
}
