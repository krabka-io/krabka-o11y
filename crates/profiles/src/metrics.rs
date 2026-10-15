//! Prometheus metrics for the profiles subsystem.
//!
//! This module uses the `OpenMetrics` `prometheus-client` crate. The binary
//! `main` constructs one cheaply-clonable [`ServiceMetrics`] bundle and threads
//! it into the distributor and querier state structs. The ingest and query
//! handler boundaries increment it with the [`ServiceMetrics::record_ingest`]
//! and [`ServiceMetrics::record_query`] helpers. The exporter emits the
//! `OpenMetrics` text format.
//!
//! This module registers counters WITHOUT a `_total` suffix.
//! `prometheus-client` appends the suffix at encode time. The registry prefix
//! is `krabka_profiles`, so the `ingest_requests` counter renders on the wire
//! as `krabka_profiles_ingest_requests_total{status="ok"}`.

pub use krabka_observability::service_metrics::{
    RouteLabel, RouteStatusLabel, SharedRegistry, StatusLabel, TenantLabel, metrics_router,
};

/// Result of one lookup in the per-pass uploaded-symbol cache, as the
/// `symbolizer_cache_requests{status}` counter records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SymbolizerCacheLookup {
    /// The build id was in the cache: `status="hit"`.
    Hit,
    /// The build id was not in the cache: `status="miss"`.
    Miss,
}

impl SymbolizerCacheLookup {
    /// The `status` label value for this lookup.
    #[must_use]
    pub const fn status(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
        }
    }
}

mod service_metrics;

pub use service_metrics::ServiceMetrics;

#[cfg(test)]
mod tests {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../metrics/tests/support/shared_registry_assertion.rs"
    ));
    use assert2::{assert, check};
    use krabka_blockstore::ObjectStoreOperation;
    use krabka_observability::service_metrics::{
        IngestRequest, QueryRequest, RequestOutcome, encode_registry,
    };
    use krabka_units::{bytes, millis};

    use super::{ServiceMetrics, StatusLabel, SymbolizerCacheLookup};

    /// A one-item, one-byte ingest request with `outcome`. Tests override
    /// the fields they check with struct update syntax.
    fn ingest_request(outcome: RequestOutcome) -> IngestRequest {
        IngestRequest {
            outcome,
            body: bytes(1),
            items: 1,
            elapsed: millis(1),
        }
    }

    #[tokio::test]
    async fn registry_has_profiles_prefix_and_all_metrics() {
        let m = ServiceMetrics::new();
        m.record_ingest(IngestRequest {
            body: bytes(1024),
            items: 3,
            elapsed: millis(12),
            ..ingest_request(RequestOutcome::Ok)
        });
        m.record_ingest(IngestRequest {
            body: bytes(0),
            items: 0,
            ..ingest_request(RequestOutcome::Error)
        });
        m.record_wal_append_failure();
        m.record_ingest_samples("tenant-a", 3);
        m.record_blocks_built(2);
        m.record_query(QueryRequest {
            route: "select_series",
            outcome: RequestOutcome::Ok,
            elapsed: millis(500),
        });
        m.record_query(QueryRequest {
            route: "render",
            outcome: RequestOutcome::Error,
            elapsed: millis(100),
        });
        m.debuginfo_upload_retries.inc();
        m.debuginfo_upload_timeouts.inc();
        m.record_symbolizer_cache(SymbolizerCacheLookup::Hit);
        m.record_symbolizer_cache(SymbolizerCacheLookup::Miss);
        let buf = assert_shared_registry_metrics!(
            &m,
            "krabka_profiles",
            [
                "krabka_profiles_ingest_requests_total",
                "krabka_profiles_ingest_bytes_total",
                "krabka_profiles_ingest_items_total",
                "krabka_profiles_ingest_duration_seconds",
                "krabka_profiles_wal_append_failures_total",
                "krabka_profiles_ingest_samples_total",
                "krabka_profiles_blocks_built_total",
                "krabka_profiles_query_requests_total",
                "krabka_profiles_query_duration_seconds",
                "krabka_profiles_debuginfo_upload_retries_total",
                "krabka_profiles_debuginfo_upload_timeouts_total",
                "krabka_profiles_symbolizer_cache_requests_total",
            ]
        );
        for label in [
            "tenant=\"tenant-a\"",
            "status=\"ok\"",
            "status=\"error\"",
            "route=\"select_series\"",
            "status=\"hit\"",
            "status=\"miss\"",
        ] {
            check!(buf.contains(label), "label {label} missing");
        }
    }

    #[test]
    fn record_ingest_adds_positive_bytes_and_items() {
        let m = ServiceMetrics::new();
        m.record_ingest(IngestRequest {
            body: bytes(1024),
            items: 3,
            elapsed: millis(12),
            ..ingest_request(RequestOutcome::Ok)
        });

        // A positive body/item count must flow through to the cumulative
        // counters, so a dropped or zeroed `inc_by` leaves these at zero.
        check!(m.ingest.bytes.get() == 1024);
        check!(m.ingest.items.get() == 3);
    }

    /// The request counter is split by outcome, so a swapped status label
    /// would report every failure as a success and vice versa. Both are
    /// recorded here and each is checked to have moved only its own series.
    #[test]
    fn ingest_requests_are_counted_under_their_own_outcome() {
        let m = ServiceMetrics::new();
        let count = |status: &str| {
            m.ingest
                .requests
                .get_or_create(&StatusLabel {
                    status: status.into(),
                })
                .get()
        };

        m.record_ingest(ingest_request(RequestOutcome::Ok));
        check!(count("ok") == 1);
        check!(count("error") == 0);

        m.record_ingest(ingest_request(RequestOutcome::Error));
        m.record_ingest(ingest_request(RequestOutcome::Error));
        check!(
            count("ok") == 1,
            "a failure must not land on the success series"
        );
        check!(count("error") == 2);
    }

    /// `record_blocks_built` returns early on zero. The guard has to reject
    /// exactly zero: inverted, it would drop every real count and record only
    /// the empty ones.
    #[test]
    fn blocks_built_counts_everything_except_zero() {
        let m = ServiceMetrics::new();

        m.record_blocks_built(0);
        check!(m.blocks_built.get() == 0, "zero adds nothing");

        m.record_blocks_built(3);
        check!(m.blocks_built.get() == 3);

        m.record_blocks_built(4);
        check!(m.blocks_built.get() == 7, "counts accumulate");
    }

    #[test]
    fn wal_append_failure_is_separate_from_request_outcome() {
        let m = ServiceMetrics::new();
        // An error request alone must NOT bump wal_append_failures.
        m.record_ingest(IngestRequest {
            body: bytes(0),
            items: 0,
            ..ingest_request(RequestOutcome::Error)
        });
        assert!(m.ingest.wal_append_failures.get() == 0);
        // Only the explicit WAL-failure call does.
        m.record_wal_append_failure();
        assert!(m.ingest.wal_append_failures.get() == 1);
    }
}
