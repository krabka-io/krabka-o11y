//! Compactor helpers for merging late-span blocks into replacement span blocks.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int32Array, Int64Array,
        ListArray, StringArray, StructArray,
    },
    compute::concat_batches,
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use futures::StreamExt;
use krabka_blockstore::{
    BlockMeta, BlockStoreError, BlockStreamWriter, BlockWriter, CompactionJob, CompactionPolicy,
    DEFAULT_BLOCK_READ_MAX, MERGE_BATCH_ROWS, MERGE_READ_BATCH_ROWS, SCOL_ATTR_KEYS,
    SCOL_ATTR_VALUE, SCOL_ATTR_VALUE_BOOL, SCOL_ATTR_VALUE_DOUBLE, SCOL_ATTR_VALUE_INT,
    SCOL_CHILD_COUNT, SCOL_DURATION_NANOS, SCOL_EVENTS, SCOL_INSTRUMENTATION_NAME,
    SCOL_INSTRUMENTATION_VERSION, SCOL_LINKS, SCOL_NAME, SCOL_NESTED_SET_LEFT,
    SCOL_NESTED_SET_RIGHT, SCOL_PARENT_ID, SCOL_PARENT_SPAN_ID, SCOL_ROOT_SERVICE_NAME,
    SCOL_ROOT_SPAN_NAME, SCOL_SPAN_ID, SCOL_START_NANO, SCOL_TRACE_DURATION_NANOS, SCOL_TRACE_ID,
    SCOL_TRACE_START_NANO, ShardedTraceBloom, SortedMerge, SummaryColumns, TraceBlockStats,
    TraceIndex, input_key_fingerprint, open_block_stream,
    plan_compactions as plan_level_compactions, span_block_decl,
    span_block_schema_with_promoted_attrs, versioned_compaction_key,
};
#[cfg(test)]
use krabka_blockstore::{read_block, span_block_schema};
use krabka_units::ByteSize;
use object_store::ObjectStore;

use crate::{
    blockbuilder::prefixed_object_key,
    error::TracesError,
    span::{
        batch::RESOURCE_ATTR_PREFIX,
        promoted::{align_batch_to_block_schema, merged_promoted_attrs},
    },
};

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_blockstore::{BlockIndex, BlockLevel};
    use object_store::{ObjectStoreExt, memory::InMemory};

    use super::*;
    use crate::span::{
        AttrValue, EventRecord, KeyValue, LinkRecord, Span, SpanKind, StatusCode, batch::span_batch,
    };

    /// `recompute_nested_sets` renumbers a trace's spans as a nested set.
    /// The tree is deliberately lopsided -- three children under the root and
    /// one grandchild -- because a balanced one lets a mutated child count
    /// coincide with the true one: with five rows and a root of three
    /// children, counting the rows that are NOT children gives two, and the
    /// two answers are only distinguishable when they differ.
    #[test]
    fn nested_sets_number_a_tree_in_preorder_and_count_each_span_children() {
        let at = |span_id: [u8; 8], parent: Option<[u8; 8]>| {
            mk_span(span_id, parent, 0, 1_000, "op", "api")
        };
        let batch = span_batch(&[
            at([1; 8], None),
            at([2; 8], Some([1; 8])),
            at([3; 8], Some([1; 8])),
            at([4; 8], Some([1; 8])),
            at([5; 8], Some([2; 8])),
        ])
        .expect("the spans form a batch");

        let out = super::recompute_nested_sets(&batch).expect("the tree is numbered");
        let ints = |name: &str| {
            out.column_by_name(name)
                .expect("the column is present")
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("the column is i32")
                .values()
                .to_vec()
        };

        // Pre-order: the root spans 1..10, its first child 2..5 with the sole
        // grandchild 3..4 inside it, then the remaining children in row order.
        check!(ints(SCOL_NESTED_SET_LEFT) == vec![1, 2, 6, 8, 3]);
        check!(ints(SCOL_NESTED_SET_RIGHT) == vec![10, 5, 7, 9, 4]);
        // A root's nested-set parent is the -1 sentinel, not a row or a zero.
        check!(ints(SCOL_PARENT_ID) == vec![-1, 1, 1, 1, 2]);
        check!(ints(SCOL_CHILD_COUNT) == vec![3, 1, 0, 0, 0]);
    }

    /// Two traces in one batch number their spans from the same counter, so a
    /// child count derived from that numbering credits each root with the
    /// other's children as well as its own. Counting where the tree is built
    /// is what keeps them apart, and this is the case that tells the two
    /// apart: both roots are numbered 1.
    #[test]
    fn child_counts_are_counted_within_each_trace_rather_than_across_the_batch() {
        let in_trace = |trace: u8, span_id: [u8; 8], parent: Option<[u8; 8]>| Span {
            trace_id: [trace; 16],
            ..mk_span(span_id, parent, 0, 1_000, "op", "api")
        };
        let batch = span_batch(&[
            in_trace(1, [1; 8], None),
            in_trace(1, [2; 8], Some([1; 8])),
            in_trace(2, [3; 8], None),
            in_trace(2, [4; 8], Some([3; 8])),
            in_trace(2, [5; 8], Some([3; 8])),
        ])
        .expect("the spans form a batch");

        let out = super::recompute_nested_sets(&batch).expect("the trees are numbered");
        let counts = out
            .column_by_name(SCOL_CHILD_COUNT)
            .expect("the column is present")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("the column is i32")
            .values()
            .to_vec();

        check!(counts == vec![1, 0, 2, 0, 0]);
    }

    /// The buffer's job is to hand on whole traces and hold back the one that
    /// might still grow, so a cut lands on a trace boundary and never inside a
    /// trace.
    #[test]
    fn the_trace_group_buffer_cuts_between_traces_and_holds_the_last_one_back() {
        let batch = |trace: u8, spans: usize| {
            span_batch(
                &(0..spans)
                    .map(|span| Span {
                        trace_id: [trace; 16],
                        ..mk_span(
                            [u8::try_from(span).expect("a small index"); 8],
                            None,
                            i64::try_from(span).expect("a small index"),
                            1,
                            "op",
                            "api",
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .expect("the spans form a batch")
        };
        let schema = span_block_schema();
        let mut buffer = TraceGroupBuffer::new(schema);

        buffer.push(batch(1, 2));
        check!(
            buffer.take_complete(4).expect("the buffer cuts").is_none(),
            "nothing is due below the row threshold"
        );
        buffer.push(batch(2, 3));
        let complete = buffer
            .take_complete(4)
            .expect("the buffer cuts")
            .expect("trace 1 is complete");

        check!(complete.num_rows() == 2, "trace 1 alone, trace 2 held back");
        check!(
            buffer.take_complete(4).expect("the buffer cuts").is_none(),
            "what is left is one still-open trace"
        );
        let rest = buffer
            .take_rest()
            .expect("the buffer drains")
            .expect("trace 2 is left");
        check!(rest.num_rows() == 3);
        check!(buffer.take_rest().expect("the buffer drains").is_none());
    }

    /// A single trace larger than the batch threshold cannot be cut, and must
    /// be kept whole rather than split into two halves that would each be
    /// numbered as a tree of their own.
    #[test]
    fn a_trace_larger_than_the_threshold_is_kept_whole() {
        let spans = (0..6_u8)
            .map(|span| mk_span([span; 8], None, i64::from(span), 1, "op", "api"))
            .collect::<Vec<_>>();
        let mut buffer = TraceGroupBuffer::new(span_block_schema());
        buffer.push(span_batch(&spans).expect("the spans form a batch"));

        check!(buffer.take_complete(2).expect("the buffer cuts").is_none());
        check!(
            buffer
                .take_rest()
                .expect("the buffer drains")
                .expect("the trace is left")
                .num_rows()
                == 6
        );
    }

    /// A span naming itself as its parent is treated as a root rather than as
    /// its own child. Without that guard the traversal re-enters the same row
    /// forever, so this case is the difference between a numbered batch and a
    /// hang.
    #[test]
    fn a_self_parented_span_is_a_root_rather_than_its_own_child() {
        let batch = span_batch(&[mk_span([1; 8], Some([1; 8]), 0, 1_000, "op", "api")])
            .expect("the span forms a batch");

        let out = super::recompute_nested_sets(&batch).expect("the self-parent is numbered");
        let ints = |name: &str| {
            out.column_by_name(name)
                .expect("the column is present")
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .expect("the column is i32")
                .values()
                .to_vec()
        };

        check!(ints(SCOL_NESTED_SET_LEFT) == vec![1]);
        check!(ints(SCOL_NESTED_SET_RIGHT) == vec![2]);
        check!(
            ints(SCOL_PARENT_ID) == vec![-1],
            "a root, not a child of itself"
        );
        check!(ints(SCOL_CHILD_COUNT) == vec![0]);
    }

    /// `MetadataValueArray::string_value` renders one cell of an Arrow column
    /// as text, and each of the four implementations formats a different type.
    /// The values are chosen so that no two render alike -- an implementation
    /// reaching the wrong column would otherwise produce something plausible.
    #[test]
    fn every_metadata_column_renders_its_own_cell_as_text() {
        let strings = StringArray::from(vec!["alpha", "beta"]);
        check!(strings.string_value(0) == "alpha");
        check!(
            strings.string_value(1) == "beta",
            "the index selects the cell"
        );

        let ints = Int64Array::from(vec![42_i64, -7]);
        check!(ints.string_value(0) == "42");
        check!(ints.string_value(1) == "-7", "a negative keeps its sign");

        let floats = Float64Array::from(vec![1.5_f64, 0.0]);
        check!(
            floats.string_value(0) == "1.5",
            "a fraction is not truncated"
        );
        check!(
            floats.string_value(1) == "0",
            "zero renders without a fraction"
        );

        let bools = BooleanArray::from(vec![true, false]);
        check!(bools.string_value(0) == "true");
        check!(bools.string_value(1) == "false", "and false is not empty");

        // No two of the four render the same text for their first cell, so an
        // implementation borrowed from a neighbouring type is visible.
        let rendered = [
            strings.string_value(0),
            ints.string_value(0),
            floats.string_value(0),
            bools.string_value(0),
        ];
        let mut unique = rendered.to_vec();
        unique.sort_unstable();
        unique.dedup();
        check!(
            unique.len() == 4,
            "the four renderings must differ: {rendered:?}"
        );
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
            span_attrs: vec![KeyValue {
                key: "env".into(),
                value: AttrValue::Str("prod".into()),
            }],
            events: vec![EventRecord {
                time_unix_nano: 1_050,
                name: "exception".into(),
                attrs: vec![KeyValue {
                    key: "cache.key".into(),
                    value: AttrValue::Str("users".into()),
                }],
            }],
            links: vec![LinkRecord {
                trace_id: [9; 16],
                span_id: [8; 8],
                attrs: vec![KeyValue {
                    key: "link.kind".into(),
                    value: AttrValue::Str("retry".into()),
                }],
            }],
            instrumentation_scope: "otel-rust".into(),
            instrumentation_version: "1.2.3".into(),
        }
    }

    fn mk_span(
        span_id: [u8; 8],
        parent: Option<[u8; 8]>,
        start_ns: i64,
        duration_ns: i64,
        name: &str,
        service: &str,
    ) -> Span {
        Span {
            trace_id: [1; 16],
            span_id,
            parent_span_id: parent,
            name: name.into(),
            kind: SpanKind::Server,
            start_ns,
            duration_ns,
            status: StatusCode::Ok,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str(service.into()),
            }],
            span_attrs: vec![],
            events: vec![],
            links: vec![],
            instrumentation_scope: "otel-rust".into(),
            instrumentation_version: "1".into(),
        }
    }

    #[tokio::test]
    async fn compaction_recomputes_trace_level_columns_across_blocks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());

        // Block A holds the root span (later start); block B holds an earlier
        // late child whose parent is NOT in B (so B's per-block root_info wrongly
        // treats the child as the root → stale `root_span_name`/`trace_start`).
        let batch_a = span_batch(&[mk_span([2; 8], None, 1_000, 100, "GET /", "api")]).unwrap();
        writer
            .write_block_with_decl(
                "tenant",
                "a.parquet",
                span_block_schema(),
                &[batch_a],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let batch_b =
            span_batch(&[mk_span([3; 8], Some([2; 8]), 800, 50, "child", "api")]).unwrap();
        writer
            .write_block_with_decl(
                "tenant",
                "b.parquet",
                span_block_schema(),
                &[batch_b],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();

        let mut index = TraceIndex::new();
        let rejected = compact_block_keys_with_max_bytes(
            store.clone(),
            &writer,
            &mut index,
            "tenant",
            &["a.parquet".to_string(), "b.parquet".to_string()],
            "rejected.parquet",
            krabka_units::bytes(1),
        )
        .await;
        assert2::assert!(rejected.is_err());

        let meta = compact_block_keys(
            store.clone(),
            &writer,
            &mut index,
            "tenant",
            &["a.parquet".to_string(), "b.parquet".to_string()],
            "compacted.parquet",
        )
        .await
        .unwrap();

        let batches = read_block(store, &meta.object_key).await.unwrap();
        let batch = &batches[0];
        check!(batch.num_rows() == 2);
        let trace_start = int64_column(batch, SCOL_TRACE_START_NANO).unwrap();
        let trace_duration = int64_column(batch, SCOL_TRACE_DURATION_NANOS).unwrap();
        let service = string_column(batch, SCOL_ROOT_SERVICE_NAME).unwrap();
        let root_name = string_column(batch, SCOL_ROOT_SPAN_NAME).unwrap();
        for row in 0..batch.num_rows() {
            // min start across both blocks, and span to the latest end.
            check!(trace_start.value(row) == 800);
            check!(trace_duration.value(row) == 300); // max(1100, 850) - 800
            // root is the true (no-parent) span, consistent across every row.
            check!(service.value(row) == "api");
            check!(root_name.value(row) == "GET /");
        }
    }

    /// A policy wide enough that only the level ladder and the fan-in cap
    /// decide what merges: no row target and no time bucketing in the way.
    fn wide_policy(max_blocks_per_job: usize, max_level: u32) -> CompactionPolicy {
        CompactionPolicy::new(
            max_blocks_per_job,
            usize::MAX,
            BlockLevel(max_level),
            i64::MAX,
        )
    }

    #[tokio::test]
    async fn a_pass_compacts_each_tenant_independently() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let mut index = TraceIndex::new();
        write_indexed_block(&writer, &mut index, "tenant-a", "tenant-a/input-1.parquet").await;
        write_indexed_block(&writer, &mut index, "tenant-a", "tenant-a/input-2.parquet").await;
        write_indexed_block(&writer, &mut index, "tenant-b", "tenant-b/input-1.parquet").await;
        write_indexed_block(&writer, &mut index, "tenant-b", "tenant-b/input-2.parquet").await;

        compact_once(store, &writer, &mut index, "", wide_policy(8, 4))
            .await
            .unwrap();

        let tenant_a = index.candidate_blocks("tenant-a", 0, 2_000);
        let tenant_b = index.candidate_blocks("tenant-b", 0, 2_000);
        assert2::assert!(tenant_a.len() == 1);
        assert2::assert!(tenant_b.len() == 1);
        check!(tenant_a[0].contains("traces/tenant-a/"));
        check!(tenant_b[0].contains("traces/tenant-b/"));
        check!(index.block_level(&tenant_a[0]) == BlockLevel(1));
    }

    /// The compactor has to stop on its own, because nothing else stops it: a
    /// scheduled pass runs forever. Each pass replaces at least two blocks
    /// with one a level higher, and a block at the top of the ladder is never
    /// an input again, so passes over a quiet index run out of work.
    #[tokio::test]
    async fn repeated_passes_climb_the_ladder_and_then_plan_nothing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let mut index = TraceIndex::new();
        for n in 0..4 {
            let key = format!("tenant/input-{n}.parquet");
            write_indexed_block(&writer, &mut index, "tenant", &key).await;
        }

        let policy = wide_policy(2, 2);
        let mut levels = Vec::new();
        let mut passes = 0;
        loop {
            let metas = compact_once(store.clone(), &writer, &mut index, "", policy)
                .await
                .unwrap();
            if metas.is_empty() {
                break;
            }
            passes += 1;
            assert2::assert!(passes <= 4, "compaction did not converge");
            levels.push(
                metas
                    .iter()
                    .map(|meta| index.block_level(&meta.object_key))
                    .collect::<Vec<_>>(),
            );
        }

        // Four blocks pair into two at level 1, then into one at level 2,
        // which is the cap: the third pass plans nothing.
        check!(levels == vec![vec![BlockLevel(1); 2], vec![BlockLevel(2)],]);
        check!(passes == 2);
        check!(index.candidate_blocks("tenant", 0, 2_000).len() == 1);
    }

    async fn write_indexed_block(
        writer: &BlockWriter,
        index: &mut TraceIndex,
        tenant: &str,
        object_key: &str,
    ) {
        let batch = span_batch(&[span()]).unwrap();
        let input = writer
            .write_block_with_decl(
                tenant,
                object_key,
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1; 16]);
        index.add_trace_block(
            tenant,
            TraceBlockStats {
                object_key: input.object_key,
                min_ts: input.min_ts,
                max_ts: input.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
                row_count: 0,
                level: BlockLevel::INGESTED,
            },
        );
    }

    #[tokio::test]
    async fn compacted_block_recomputes_tag_metadata_from_rows() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let batch = span_batch(&[span()]).unwrap();
        let input = writer
            .write_block_with_decl(
                "tenant",
                "input.parquet",
                span_block_schema(),
                &[batch],
                &span_block_decl(),
                SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
            )
            .await
            .unwrap();

        let mut bloom = ShardedTraceBloom::with_tempo_defaults(1);
        bloom.insert(&[1; 16]);
        let mut index = TraceIndex::new();
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: input.object_key.clone(),
                min_ts: input.min_ts,
                max_ts: input.max_ts,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
                row_count: 0,
                level: BlockLevel::INGESTED,
            },
        );

        compact_block_keys(
            store,
            &writer,
            &mut index,
            "tenant",
            std::slice::from_ref(&input.object_key),
            "compacted.parquet",
        )
        .await
        .unwrap();

        let names = index.tag_names("tenant", 0, 2_000);
        for name in [
            "service.name",
            "env",
            "instrumentation:name",
            "event:name",
            "event:timeSinceStart",
            "cache.key",
            "link:traceID",
            "link:spanID",
            "link.kind",
        ] {
            check!(names.contains(&name.to_string()));
        }
        for (tag, want) in [
            ("service.name", "api"),
            ("env", "prod"),
            ("instrumentation:name", "otel-rust"),
            ("event:name", "exception"),
            ("event:timeSinceStart", "50"),
            ("cache.key", "users"),
            ("link:traceID", "09090909090909090909090909090909"),
            ("link:spanID", "0808080808080808"),
            ("link.kind", "retry"),
        ] {
            check!(index.tag_values("tenant", tag, 0, 2_000) == vec![want.to_string()]);
        }
    }

    #[tokio::test]
    async fn a_reused_trace_input_key_cannot_overwrite_an_existing_compaction_output() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let input_key = "reused.parquet";

        let write_input = |name: &'static str| {
            let writer = &writer;
            async move {
                writer
                    .write_block_with_decl(
                        "tenant",
                        input_key,
                        span_block_schema(),
                        &[span_batch(&[mk_span([1; 8], None, 1_000, 10, name, "api")])
                            .expect("the span forms a batch")],
                        &span_block_decl(),
                        SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
                    )
                    .await
                    .expect("the input block is written");
            }
        };
        write_input("old").await;
        let mut old_index = TraceIndex::new();
        let old = compact_block_keys(
            store.clone(),
            &writer,
            &mut old_index,
            "tenant",
            &[input_key.to_string()],
            "compacted.parquet",
        )
        .await
        .expect("the old input compacts");
        let old_bytes = store
            .get(&object_store::path::Path::from(old.object_key.clone()))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();

        write_input("new").await;
        let mut new_index = TraceIndex::new();
        let new = compact_block_keys(
            store.clone(),
            &writer,
            &mut new_index,
            "tenant",
            &[input_key.to_string()],
            "compacted.parquet",
        )
        .await
        .expect("the replacement input compacts");

        check!(old.object_key != new.object_key);
        check!(
            store
                .get(&object_store::path::Path::from(old.object_key))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                == old_bytes,
            "the stale pass's object remains untouched"
        );
    }

    /// The declared trace order is `[trace_id, start_unix_nano]`, so two
    /// sibling spans starting on the same nanosecond are rows the key cannot
    /// separate -- and their row order is exactly what `recompute_nested_sets`
    /// numbers them in. A merge that let the run holding a trace's earlier
    /// rows carry on through the tie would renumber the pair against what the
    /// pre-compaction blocks showed, a difference `/api/traces` hands straight
    /// to Grafana. The tie belongs to the earliest input block, as it does
    /// anywhere else the declared key runs out.
    #[tokio::test]
    async fn sibling_spans_tied_on_the_sort_key_keep_their_block_order_through_a_compaction() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let writer = BlockWriter::new(store.clone());
        let write = |key: &'static str, spans: Vec<Span>| {
            let writer = &writer;
            async move {
                let batch = span_batch(&spans).expect("the spans form a batch");
                writer
                    .write_block_with_decl(
                        "tenant",
                        key,
                        span_block_schema(),
                        &[batch],
                        &span_block_decl(),
                        SummaryColumns::new(SCOL_TRACE_ID, SCOL_START_NANO),
                    )
                    .await
                    .expect("the block is written");
            }
        };

        write(
            "tied-a.parquet",
            vec![mk_span([4; 8], Some([2; 8]), 2_000, 10, "a", "api")],
        )
        .await;
        // The second block also holds the root, whose earlier start makes it
        // the run the merge picks first -- the shape in which a merge can
        // over-run a tie it did not win.
        write(
            "tied-b.parquet",
            vec![
                mk_span([2; 8], None, 1_000, 100, "GET /", "api"),
                mk_span([3; 8], Some([2; 8]), 2_000, 10, "b", "api"),
            ],
        )
        .await;

        let mut index = TraceIndex::new();
        let meta = compact_block_keys(
            store.clone(),
            &writer,
            &mut index,
            "tenant",
            &["tied-a.parquet".to_string(), "tied-b.parquet".to_string()],
            "tied.parquet",
        )
        .await
        .expect("the blocks compact");

        let batches = read_block(store, &meta.object_key)
            .await
            .expect("the block reads");
        let tied = &batches[0];
        let span_ids = fixed_column(tied, SCOL_SPAN_ID, 8).expect("the column is present");
        let left = tied
            .column_by_name(SCOL_NESTED_SET_LEFT)
            .expect("the column is present")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("the column is i32");
        let numbering = (0..tied.num_rows())
            .map(|row| (span_ids.value(row)[0], left.value(row)))
            .collect::<Vec<_>>();

        check!(
            numbering == vec![(2, 1), (4, 2), (3, 4)],
            "the root, then the tied sibling from the earlier block, then the later block's"
        );
    }
}

mod attr_value;
mod boolean_array;
mod collect_attr_metadata;
mod collect_event_metadata;
mod collect_link_metadata;
mod collect_nested_attrs;
mod collect_nested_metadata;
mod collect_string_column_metadata;
mod compact_block_keys;
mod compact_block_keys_with_max_bytes;
mod compact_once;
mod compact_once_with_policy;
mod compacted_block_stats;
mod first_string_list_value;
mod fixed_column;
mod float64_array;
mod insert_tag_value;
mod int64_array;
mod int64_column;
mod list_column;
mod metadata_value_array;
mod optional_list_column;
mod plan_compactions;
mod planned_compacted_object_key;
mod push_tag_metadata;
mod push_trace_ids;
mod recompute_nested_sets;
mod recompute_trace_level_columns;
mod replace_int32_columns;
mod set_column;
mod string_array;
mod string_column;
mod string_list_value;
mod struct_fixed_field;
mod struct_i64_field;
mod struct_list_field;
mod struct_string_field;
mod trace_group_buffer;

use attr_value::attr_value;
use collect_attr_metadata::collect_attr_metadata;
use collect_event_metadata::collect_event_metadata;
use collect_link_metadata::collect_link_metadata;
use collect_nested_attrs::collect_nested_attrs;
use collect_nested_metadata::collect_nested_metadata;
use collect_string_column_metadata::collect_string_column_metadata;
pub use compact_block_keys::compact_block_keys;
pub use compact_block_keys_with_max_bytes::compact_block_keys_with_max_bytes;
pub use compact_once::compact_once;
pub use compact_once_with_policy::compact_once_with_policy;
use compacted_block_stats::CompactedBlockStats;
use first_string_list_value::first_string_list_value;
use fixed_column::fixed_column;
use insert_tag_value::insert_tag_value;
use int64_column::int64_column;
use list_column::list_column;
use metadata_value_array::MetadataValueArray;
use optional_list_column::optional_list_column;
pub use plan_compactions::plan_compactions;
pub use planned_compacted_object_key::planned_compacted_object_key;
use push_tag_metadata::push_tag_metadata;
use push_trace_ids::push_trace_ids;
use recompute_nested_sets::recompute_nested_sets;
use recompute_trace_level_columns::recompute_trace_level_columns;
use replace_int32_columns::replace_int32_columns;
use set_column::set_column;
use string_column::string_column;
use string_list_value::string_list_value;
use struct_fixed_field::struct_fixed_field;
use struct_i64_field::struct_i64_field;
use struct_list_field::struct_list_field;
use struct_string_field::struct_string_field;
use trace_group_buffer::TraceGroupBuffer;
