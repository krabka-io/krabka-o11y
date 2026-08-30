//! Block-builder helpers for turning WAL span records into span blocks.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::compute::concat_batches;
use krabka_blockstore::{
    BlockMeta, BlockWriter, IndexSnapshotRetain, PromotedSpanAttr, SCOL_START_NANO, SCOL_TRACE_ID,
    ShardedTraceBloom, SummaryColumns, TraceBlockStats, TraceIndex, span_block_decl,
    span_block_schema_with_promoted_attrs,
};
use krabka_client_consumer::{Consumer, ConsumerRecord};
use krabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
    secs,
};
use object_store::ObjectStore;
use tokio::{sync::Mutex, time::Instant};
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::{
    error::TracesError,
    ids::{MaxOffset, MinOffset, WindowStartNs},
    metrics::ServiceMetrics,
    span::{AttrValue, Span, batch::span_batch_with_promoted_attrs},
    wal::SpanRecord,
};

#[cfg(test)]
mod tests {

    use super::*;
    use crate::span::{EventRecord, KeyValue, LinkRecord, SpanKind, StatusCode};

    /// `set_remote_parent_from_records` must re-parent the span into the trace
    /// carried on the FIRST record whose header key equals `TRACEPARENT_HEADER`.
    ///
    /// This test guards two mutants. The first replaces the whole function with
    /// `()`, and the span then keeps its own fresh trace id instead of the
    /// header's. The second flips the header-key comparison from `==` to `!=`,
    /// and the non-traceparent record then matches first, so its absent or
    /// garbage context fails to re-parent the span. The non-traceparent record
    /// sits BEFORE the traceparent one on purpose, so the `==`/`!=` difference
    /// is observable.
    #[test]
    fn set_remote_parent_from_records_reparents_into_header_trace() {
        use opentelemetry::trace::{TraceContextExt as _, TraceId, TracerProvider as _};
        use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;
        use tracing_subscriber::prelude::*;

        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let provider = SdkTracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .build();
        let tracer = provider.tracer("blockbuilder-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            fn record(headers: Vec<krabka_client_consumer::Header>) -> ConsumerRecord {
                ConsumerRecord {
                    topic: "wal".into(),
                    partition: 0,
                    offset: 0,
                    leader_epoch: 0,
                    timestamp: 0,
                    key: None,
                    value: None,
                    headers,
                }
            }

            let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
            let records = vec![
                // A record WITHOUT the traceparent header comes first: with the
                // `==`→`!=` mutant, `find` would (wrongly) select this one and
                // extract no valid context, so the trace-id assertion would fail.
                record(vec![krabka_client_consumer::Header {
                    key: "other".into(),
                    value: Some(bytes::Bytes::from_static(b"x")),
                }]),
                // The record actually carrying the producer's W3C trace context.
                record(vec![krabka_client_consumer::Header {
                    key: TRACEPARENT_HEADER.into(),
                    value: Some(bytes::Bytes::from(traceparent.as_bytes().to_vec())),
                }]),
            ];

            let span = tracing::info_span!("t");
            set_remote_parent_from_records(&span, &records);

            // The span now belongs to the producer's trace (shares its trace id).
            // A no-op mutant leaves the span in its own fresh trace, so this fails.
            let sc = span.context().span().span_context().clone();
            assert2::assert!(
                sc.trace_id() == TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap()
            );
        });
    }

    fn span() -> Span {
        Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "GET /".into(),
            kind: SpanKind::Server,
            start_ns: 1_000,
            duration_ns: 100,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: Vec::new(),
            events: vec![EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: Vec::new(),
            }],
            links: vec![LinkRecord {
                trace_id: [9; 16],
                span_id: [8; 8],
                attrs: Vec::new(),
            }],
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        }
    }

    #[test]
    fn collect_tags_indexes_event_and_link_intrinsics() {
        let mut tag_names = BTreeSet::new();
        let mut tag_values = BTreeMap::new();

        collect_tags(&[span()], &mut tag_names, &mut tag_values);

        assert2::assert!(
            tag_names
                == BTreeSet::from([
                    "event:name".to_string(),
                    "event:timeSinceStart".to_string(),
                    "link:spanID".to_string(),
                    "link:traceID".to_string(),
                    "service.name".to_string(),
                ])
        );
        assert2::assert!(
            tag_values
                == BTreeMap::from([
                    (
                        "event:name".to_string(),
                        BTreeSet::from(["exception".to_string()])
                    ),
                    (
                        "event:timeSinceStart".to_string(),
                        BTreeSet::from(["50".to_string()])
                    ),
                    (
                        "link:spanID".to_string(),
                        BTreeSet::from(["0808080808080808".to_string()])
                    ),
                    (
                        "link:traceID".to_string(),
                        BTreeSet::from(["09090909090909090909090909090909".to_string()])
                    ),
                    (
                        "service.name".to_string(),
                        BTreeSet::from(["api".to_string()])
                    ),
                ])
        );
    }

    #[test]
    fn collect_tags_indexes_event_and_link_attributes() {
        let mut span = span();
        span.events[0].attrs = vec![KeyValue {
            key: "cache.key".into(),
            value: AttrValue::Str("users".into()),
        }];
        span.links[0].attrs = vec![KeyValue {
            key: "link.kind".into(),
            value: AttrValue::Str("retry".into()),
        }];
        let mut tag_names = BTreeSet::new();
        let mut tag_values = BTreeMap::new();

        collect_tags(&[span], &mut tag_names, &mut tag_values);

        assert2::assert!(
            tag_names
                == BTreeSet::from([
                    "cache.key".to_string(),
                    "event:name".to_string(),
                    "event:timeSinceStart".to_string(),
                    "link.kind".to_string(),
                    "link:spanID".to_string(),
                    "link:traceID".to_string(),
                    "service.name".to_string(),
                ])
        );
        assert2::assert!(
            tag_values
                == BTreeMap::from([
                    (
                        "cache.key".to_string(),
                        BTreeSet::from(["users".to_string()])
                    ),
                    (
                        "event:name".to_string(),
                        BTreeSet::from(["exception".to_string()])
                    ),
                    (
                        "event:timeSinceStart".to_string(),
                        BTreeSet::from(["50".to_string()])
                    ),
                    (
                        "link.kind".to_string(),
                        BTreeSet::from(["retry".to_string()])
                    ),
                    (
                        "link:spanID".to_string(),
                        BTreeSet::from(["0808080808080808".to_string()])
                    ),
                    (
                        "link:traceID".to_string(),
                        BTreeSet::from(["09090909090909090909090909090909".to_string()])
                    ),
                    (
                        "service.name".to_string(),
                        BTreeSet::from(["api".to_string()])
                    ),
                ])
        );
    }

    #[test]
    fn collect_tags_indexes_instrumentation_intrinsics() {
        let mut span = span();
        span.instrumentation_scope = "otel-rust".into();
        span.instrumentation_version = "1.2.3".into();
        let mut tag_names = BTreeSet::new();
        let mut tag_values = BTreeMap::new();

        collect_tags(&[span], &mut tag_names, &mut tag_values);

        assert2::assert!(
            tag_names
                == BTreeSet::from([
                    "event:name".to_string(),
                    "event:timeSinceStart".to_string(),
                    "instrumentation:name".to_string(),
                    "instrumentation:version".to_string(),
                    "link:spanID".to_string(),
                    "link:traceID".to_string(),
                    "service.name".to_string(),
                ])
        );
        assert2::assert!(
            tag_values
                == BTreeMap::from([
                    (
                        "event:name".to_string(),
                        BTreeSet::from(["exception".to_string()])
                    ),
                    (
                        "event:timeSinceStart".to_string(),
                        BTreeSet::from(["50".to_string()])
                    ),
                    (
                        "instrumentation:name".to_string(),
                        BTreeSet::from(["otel-rust".to_string()])
                    ),
                    (
                        "instrumentation:version".to_string(),
                        BTreeSet::from(["1.2.3".to_string()])
                    ),
                    (
                        "link:spanID".to_string(),
                        BTreeSet::from(["0808080808080808".to_string()])
                    ),
                    (
                        "link:traceID".to_string(),
                        BTreeSet::from(["09090909090909090909090909090909".to_string()])
                    ),
                    (
                        "service.name".to_string(),
                        BTreeSet::from(["api".to_string()])
                    ),
                ])
        );
    }
}

mod attr_value_string;
mod block_build_options;
mod block_builder_config;
mod build_blocks;
mod build_blocks_with_options;
mod build_blocks_with_prefix;
mod build_blocks_with_promoted_attrs;
mod collect_tags;
mod consumer;
mod decode_consumer_records;
mod default_flush_max_age;
mod default_flush_max_records;
mod flush_accumulator;
mod flush_and_commit;
mod flush_partition_windows;
mod group_by_trace;
mod insert_tag_value;
mod object_key;
mod partition_window;
mod prefixed_object_key;
mod run;
mod set_remote_parent_from_records;
mod tenants_in_records;
mod traceparent_header;
mod wal_consumer_commit;
mod wal_consumer_poll;

use attr_value_string::attr_value_string;
use block_build_options::BlockBuildOptions;
pub use block_builder_config::BlockBuilderConfig;
pub use build_blocks::build_blocks;
use build_blocks_with_options::build_blocks_with_options;
pub use build_blocks_with_prefix::build_blocks_with_prefix;
pub use build_blocks_with_promoted_attrs::build_blocks_with_promoted_attrs;
use collect_tags::collect_tags;
pub use decode_consumer_records::decode_consumer_records;
pub use default_flush_max_age::DEFAULT_FLUSH_MAX_AGE;
pub use default_flush_max_records::DEFAULT_FLUSH_MAX_RECORDS;
pub use flush_accumulator::FlushAccumulator;
use flush_and_commit::flush_and_commit;
pub use flush_partition_windows::flush_partition_windows;
pub use group_by_trace::group_by_trace;
use insert_tag_value::insert_tag_value;
pub use object_key::object_key;
pub use partition_window::PartitionWindow;
pub use prefixed_object_key::prefixed_object_key;
pub use run::run;
use set_remote_parent_from_records::set_remote_parent_from_records;
use tenants_in_records::tenants_in_records;
use traceparent_header::TRACEPARENT_HEADER;
pub use wal_consumer_commit::WalConsumerCommit;
pub use wal_consumer_poll::WalConsumerPoll;
