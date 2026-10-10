//! The querier configuration that the `cli` and `environment` suites both
//! spell out, one with flags and the other with environment variables.

use krabka_observability::{QuerierIndexSource, Role, ServiceConfig};
use krabka_units::{bytes, kibibytes, nanos};

/// A querier on `127.0.0.1:3200` over tenant-a's shards in
/// `s3://krabka-observability`, scoped to 10 ns through 30 ns, with every
/// other setting at its default.
pub fn scoped_querier_config() -> ServiceConfig {
    ServiceConfig {
        target: Role::Querier,
        listen_addr: "127.0.0.1:3200".parse().unwrap(),
        object_store_url: Some("s3://krabka-observability".to_string()),
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
        ..ServiceConfig::default()
    }
}
