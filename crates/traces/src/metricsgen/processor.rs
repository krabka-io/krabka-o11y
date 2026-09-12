//! Metrics-generator processor orchestration.

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
};

use crate::metricsgen::{
    checkpoint::CheckpointCodecError,
    clock::Clock,
    config::MetricsGenConfig,
    contract::SpanRecord,
    series::SeriesPayload,
    servicegraph::{EdgeStore, RecordOutcome},
    spanmetrics::SpanMetricsRegistry,
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::check;
    use krabka_units::{ByteSize, convert::ByteSizeExt as _};

    use super::*;
    use crate::metricsgen::{
        clock::MockClock,
        config::MetricsGenConfig,
        contract::{SpanKind, SpanRecord, StatusCode},
    };

    fn span(
        tenant: &str,
        service: &str,
        kind: SpanKind,
        span_id: [u8; 8],
        parent: [u8; 8],
    ) -> SpanRecord {
        SpanRecord {
            tenant: tenant.into(),
            trace_id: [0x33; 16],
            span_id,
            parent_span_id: parent,
            name: "op".into(),
            kind,
            start_ns: 0,
            duration_ns: 5_000_000,
            status: StatusCode::Ok,
            status_message: String::new(),
            service_name: service.into(),
            attributes: vec![],
            resource_attributes: vec![],
            size: ByteSize::from_bytes(10),
        }
    }

    #[tokio::test]
    async fn process_then_collect_emits_both_processors_per_tenant() {
        let clock = MockClock::new(0);
        let mut generator = MetricsGenerator::new(MetricsGenConfig::default(), Arc::new(clock));

        generator.process(&span("A", "frontend", SpanKind::Client, [0xA; 8], [0; 8]));
        generator.process(&span("A", "backend", SpanKind::Server, [0xB; 8], [0xA; 8]));
        generator.process(&span("B", "svc", SpanKind::Server, [0xC; 8], [0; 8]));

        let payloads = generator.collect(1_000);
        assert2::assert!(payloads.len() == 2);

        let a = payloads.iter().find(|p| p.tenant == "A").unwrap();
        assert2::assert!(
            a.series
                .iter()
                .any(|s| s.name == "traces_service_graph_request_total")
        );
        assert2::assert!(
            a.series
                .iter()
                .any(|s| s.name == "traces_spanmetrics_calls_total")
        );

        let b = payloads.iter().find(|p| p.tenant == "B").unwrap();
        assert2::assert!(
            b.series
                .iter()
                .any(|s| s.name == "traces_spanmetrics_calls_total")
        );
        assert2::assert!(
            !b.series
                .iter()
                .any(|s| s.name == "traces_service_graph_request_total")
        );
    }

    /// The tenant map is the third unbounded map of the generator, and one WAL
    /// topic carries every tenant. A new tenant is refused once the map is
    /// full, and the spans of the tenants already held keep flowing.
    #[tokio::test]
    async fn tenant_map_full_refuses_new_tenants() {
        let cfg = MetricsGenConfig {
            max_tenants: 2,
            ..MetricsGenConfig::default()
        };
        let mut generator = MetricsGenerator::new(cfg, Arc::new(MockClock::new(0)));

        check!(
            generator.process(&span("A", "frontend", SpanKind::Server, [0xA; 8], [0; 8]))
                == RecordOutcome::Recorded
        );
        check!(
            generator.process(&span("B", "frontend", SpanKind::Server, [0xB; 8], [0; 8]))
                == RecordOutcome::Recorded
        );
        check!(
            generator.process(&span("C", "frontend", SpanKind::Server, [0xC; 8], [0; 8]))
                == RecordOutcome::Dropped
        );
        // A tenant already held is unaffected by the map being full.
        check!(
            generator.process(&span("A", "frontend", SpanKind::Server, [0xD; 8], [0; 8]))
                == RecordOutcome::Recorded
        );

        check!(generator.discarded_tenant_spans() == 1);
        let payloads = generator.collect(1_000);
        let mut tenants: Vec<String> = payloads.iter().map(|p| p.tenant.clone()).collect();
        tenants.sort();
        check!(tenants == vec!["A".to_string(), "B".to_string()]);
    }

    /// Zero is "no cap", as it is for every other limit here.
    #[tokio::test]
    async fn a_zero_tenant_cap_is_unlimited() {
        let cfg = MetricsGenConfig {
            max_tenants: 0,
            ..MetricsGenConfig::default()
        };
        let mut generator = MetricsGenerator::new(cfg, Arc::new(MockClock::new(0)));

        for tenant in ["A", "B", "C", "D"] {
            check!(
                generator.process(&span(tenant, "svc", SpanKind::Server, [0xA; 8], [0; 8]))
                    == RecordOutcome::Recorded
            );
        }

        check!(generator.discarded_tenant_spans() == 0);
        check!(generator.collect(1_000).len() == 4);
    }

    #[tokio::test]
    async fn collect_expires_stale_edges_via_clock() {
        let clock = MockClock::new(0);
        let mut generator =
            MetricsGenerator::new(MetricsGenConfig::default(), Arc::new(clock.clone()));
        generator.process(&span("A", "frontend", SpanKind::Client, [0xA; 8], [0; 8]));

        clock.set(11_000_000_000);
        let payloads = generator.collect(2_000);
        let a = payloads.iter().find(|p| p.tenant == "A").unwrap();
        assert2::assert!(
            a.series
                .iter()
                .any(|s| s.name == "traces_service_graph_unpaired_spans_total")
        );
    }
}

mod edge_checkpoint_entry;
mod metrics_generator;
mod tenant_edge_checkpoints;
mod tenant_state;

pub use edge_checkpoint_entry::EdgeCheckpointEntry;
pub use metrics_generator::MetricsGenerator;
pub use tenant_edge_checkpoints::TenantEdgeCheckpoints;
use tenant_state::TenantState;
