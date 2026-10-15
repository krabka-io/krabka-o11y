//! Block-builder helpers for turning WAL span records into span blocks.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow::{compute::concat_batches, record_batch::RecordBatch};
use krabka_blockstore::{
    BlockMeta, BlockWriter, IndexSnapshotRetain, ObjectStoreRetryPolicy, OffsetRange,
    PromotedSpanAttr, RetryingObjectStore, SCOL_START_NANO, SCOL_TRACE_ID, ShardedTraceBloom,
    SummaryColumns, TraceBlockStats, TraceIndex, escape_object_path_segment, span_block_decl,
    span_block_schema_with_promoted_attrs,
};
use krabka_client_consumer::{Consumer, ConsumerRecord};
use krabka_units::{
    Time,
    convert::{StdDurationExt as _, TimeExt as _},
    secs,
};
use object_store::ObjectStore;
pub(crate) use tags::TagCatalog;
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
#[path = "../../observability/tests/support/recording_trace_subscriber.rs"]
mod recording_trace_subscriber;

#[cfg(test)]
mod tests {

    use super::*;
    use crate::span::{EventRecord, KeyValue, LinkRecord, test_span::api_server_span};

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
        use opentelemetry::trace::{TraceContextExt as _, TraceId};
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        super::recording_trace_subscriber::with_recording_trace("blockbuilder-test", || {
            fn record(headers: Vec<krabka_client_consumer::Header>) -> ConsumerRecord {
                ConsumerRecord {
                    topic: "wal".into(),
                    partition: 0,
                    offset: 0,
                    leader_epoch: 0,
                    timestamp: 0,
                    timestamp_type: krabka_client_consumer::TimestampType::CreateTime,
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
            ..api_server_span()
        }
    }

    /// One tag and one value that `collect_tags` indexes for it.
    struct IndexedTag {
        tag: &'static str,
        value: &'static str,
    }

    /// The tags `span()` indexes: its event and link intrinsics and its service.
    const SPAN_TAGS: [IndexedTag; 5] = [
        IndexedTag {
            tag: "event:name",
            value: "exception",
        },
        IndexedTag {
            tag: "event:timeSinceStart",
            value: "50",
        },
        IndexedTag {
            tag: "link:spanID",
            value: "0808080808080808",
        },
        IndexedTag {
            tag: "link:traceID",
            value: "09090909090909090909090909090909",
        },
        IndexedTag {
            tag: "service.name",
            value: "api",
        },
    ];

    /// Asserts that `collect_tags` over `span` indexes exactly the tags of
    /// `span()` plus `extra_tags`, each with its one value.
    fn assert_collects_span_tags_and(span: Span, extra_tags: &[IndexedTag]) {
        let mut tag_names = BTreeSet::new();
        let mut tag_values = BTreeMap::new();

        collect_tags(&[span], &mut tag_names, &mut tag_values);

        let mut expected_values = BTreeMap::new();
        for indexed in SPAN_TAGS.iter().chain(extra_tags) {
            expected_values.insert(
                indexed.tag.to_string(),
                BTreeSet::from([indexed.value.to_string()]),
            );
        }
        let expected_names = expected_values.keys().cloned().collect::<BTreeSet<_>>();
        assert2::assert!(tag_names == expected_names);
        assert2::assert!(tag_values == expected_values);
    }

    #[test]
    fn collect_tags_indexes_event_and_link_intrinsics() {
        assert_collects_span_tags_and(span(), &[]);
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

        assert_collects_span_tags_and(
            span,
            &[
                IndexedTag {
                    tag: "cache.key",
                    value: "users",
                },
                IndexedTag {
                    tag: "link.kind",
                    value: "retry",
                },
            ],
        );
    }

    #[test]
    fn collect_tags_indexes_instrumentation_intrinsics() {
        let mut span = span();
        span.instrumentation_scope = "otel-rust".into();
        span.instrumentation_version = "1.2.3".into();

        assert_collects_span_tags_and(
            span,
            &[
                IndexedTag {
                    tag: "instrumentation:name",
                    value: "otel-rust",
                },
                IndexedTag {
                    tag: "instrumentation:version",
                    value: "1.2.3",
                },
            ],
        );
    }
}
mod build_blocks;
mod flush;
mod group_by_trace;
mod run;
mod settings;
mod tags;
mod wal_consumer;
use build_blocks::build_blocks_with_options;
pub use build_blocks::{build_blocks_with_prefix, build_blocks_with_promoted_attrs};
use flush::flush_and_commit;
pub use flush::{FlushAccumulator, PartitionWindow, flush_partition_windows};
pub use group_by_trace::{TraceIdentity, group_by_trace, group_by_trace_in_record_order};
pub use run::run;
pub use settings::{BlockBuilderConfig, DEFAULT_FLUSH_MAX_AGE, TRACE_BLOCK_OBJECT_PREFIX};
pub(crate) use settings::BlockBuildOptions;
use tags::collect_tags;
pub use wal_consumer::{
    BlockBuilderConsumer, WalConsumerCommit, WalConsumerPoll, decode_consumer_records,
};

/// Default number of buffered span records that triggers a flush.
pub const DEFAULT_FLUSH_MAX_RECORDS: usize = 50_000;

/// W3C trace-context header key that the distributor's ingest span puts on WAL
/// records. The consume side uses it to continue the same distributed trace.
pub(crate) const TRACEPARENT_HEADER: &str = "traceparent";

pub(crate) fn attr_value_string(value: &AttrValue) -> String {
    match value {
        AttrValue::Str(value) => value.clone(),
        AttrValue::Int(value) => value.to_string(),
        AttrValue::Double(value) => value.to_string(),
        AttrValue::Bool(value) => value.to_string(),
        AttrValue::Bytes(value) => hex::encode(value),
        AttrValue::Array(_) | AttrValue::Unsupported(_) => value.otlp_json().to_string(),
    }
}

/// Apply an optional object-store prefix to a raw traces object key.
#[must_use]
pub fn prefixed_object_key(prefix: &str, key: &str) -> String {
    let prefix = prefix.trim_matches('/');
    let key = key.trim_start_matches('/');
    if prefix.is_empty() {
        key.to_string()
    } else {
        format!("{prefix}/{key}")
    }
}

pub(crate) fn tenants_in_records(records: &[SpanRecord]) -> BTreeSet<String> {
    records.iter().map(|record| record.tenant.clone()).collect()
}

/// Build and write one span block for `tenant` from the supplied WAL records.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn build_blocks(
    writer: &BlockWriter,
    index: &mut TraceIndex,
    tenant: &str,
    partition: i32,
    records: &[SpanRecord],
    offset_range: OffsetRange,
) -> Result<Vec<BlockMeta>, TracesError> {
    build_blocks_with_promoted_attrs(writer, index, tenant, partition, records, offset_range, &[])
        .await
}

/// Deterministic object key for one block-builder flush window.
///
/// The tenant is one path segment, escaped with
/// [`krabka_blockstore::escape_object_path_segment`]. A tenant id can hold
/// characters that `object_store` rewrites, so the raw name is not always the
/// segment the store lists.
#[must_use]
pub fn object_key(
    tenant: &str,
    partition: i32,
    min_offset: MinOffset,
    max_offset: MaxOffset,
    window_start_ns: WindowStartNs,
) -> String {
    let tenant = escape_object_path_segment(tenant);
    format!(
        "{TRACE_BLOCK_OBJECT_PREFIX}/{tenant}/{partition:05}/\
         {min_offset:020}-{max_offset:020}-{window_start_ns}.parquet",
        min_offset = min_offset.0,
        max_offset = max_offset.0,
        window_start_ns = window_start_ns.0,
    )
}

/// Make `span` a child of the distributed trace carried on any consumed
/// record's `traceparent` header.
///
/// This uses the first record that carries such a header. It does nothing when
/// no record carries one, for example when an unsampled ingest request produced
/// the records.
pub(crate) fn set_remote_parent_from_records(span: &tracing::Span, records: &[ConsumerRecord]) {
    let Some(record) = records
        .iter()
        .find(|record| record.headers.iter().any(|h| h.key == TRACEPARENT_HEADER))
    else {
        return;
    };
    krabka_telemetry::propagation::set_remote_parent(
        span,
        record
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_deref().unwrap_or(&[][..]))),
    );
}
