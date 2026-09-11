use std::sync::Arc;

use arrow::{
    array::{Array, DictionaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, StringArray},
    datatypes::Int32Type,
    record_batch::RecordBatch,
};
use assert2::check;
use krabka_blockstore::{
    BlockLevel, BlockMeta, BlockWriter, CompactionJob, PromotedSpanAttr, SCOL_START_NANO,
    SCOL_TRACE_ID, TraceIndex, level_above, read_block, unescape_object_path_segment,
};
use krabka_traces::{
    AttrValue, KeyValue, Span, SpanKind, SpanRecord, StatusCode,
    blockbuilder::{build_blocks, build_blocks_with_promoted_attrs},
    compactor::{compact_block_keys, planned_compacted_object_key},
};
use object_store::{ObjectStore, memory::InMemory, path::Path};

fn span(trace_id: [u8; 16], span_id: u8, parent: Option<u8>, start_ns: i64) -> Span {
    Span {
        trace_id,
        span_id: [span_id; 8],
        parent_span_id: parent.map(|id| [id; 8]),
        name: format!("span-{span_id}"),
        kind: SpanKind::Server,
        start_ns,
        duration_ns: 5,
        status: StatusCode::Ok,
        status_message: String::new(),
        resource_attrs: vec![KeyValue {
            key: "service.name".into(),
            value: AttrValue::Str("api".into()),
        }],
        span_attrs: vec![KeyValue {
            key: "http.method".into(),
            value: AttrValue::Str("GET".into()),
        }],
        events: Vec::new(),
        links: Vec::new(),
        instrumentation_scope: "test".into(),
        instrumentation_version: String::new(),
    }
}

/// The input keys and the output key of the compaction production would plan
/// over `inputs`.
///
/// The compactor names its output with `planned_compacted_object_key`, which
/// folds the input keys into the name so two jobs over the same tenant and time
/// range cannot mint the same key and clobber each other's object in the store.
/// Deriving the key any other way here would let these tests pass on a key
/// shape production never writes.
fn planned_job_keys(tenant: &str, inputs: &[&BlockMeta]) -> (Vec<String>, String) {
    let job = CompactionJob {
        tenant: tenant.to_string(),
        input_keys: inputs.iter().map(|meta| meta.object_key.clone()).collect(),
        output_level: level_above(inputs.iter().map(|meta| meta.level)),
        min_ts: inputs
            .iter()
            .map(|meta| meta.min_ts)
            .min()
            .unwrap_or_default(),
        max_ts: inputs
            .iter()
            .map(|meta| meta.max_ts)
            .max()
            .unwrap_or_default(),
        row_count: inputs.iter().map(|meta| meta.row_count).sum(),
    };
    let output_key = planned_compacted_object_key(&job);
    (job.input_keys, output_key)
}

/// The compacted key escapes its tenant as the block-builder key does. Each
/// row must stay one path segment that the store keeps byte for byte, and must
/// read back as the tenant the job named.
#[test]
fn a_compacted_key_escapes_the_tenant_into_one_segment_that_reads_back() {
    let cases = [
        ("plain", "tenant-a", "tenant-a"),
        ("star", "a*b", "a!2Ab"),
        ("escape marker", "a!b", "a!21b"),
        ("quote", "a'b", "a!27b"),
    ];

    for (name, tenant, segment) in cases {
        let key = planned_compacted_object_key(&CompactionJob {
            tenant: tenant.to_string(),
            input_keys: vec!["in-1".into(), "in-2".into()],
            output_level: BlockLevel::INGESTED.next(),
            min_ts: 10,
            max_ts: 20,
            row_count: 2,
        });
        check!(
            key.starts_with(&format!("traces/{segment}/compacted/l1-10-20-")),
            "{name}: {key}"
        );
        let path = Path::from(key.as_str());
        check!(
            path.as_ref() == key,
            "{name}: the store keeps the key as written"
        );
        let parts: Vec<_> = path.parts().collect();
        check!(parts.len() == 4, "{name}");
        check!(
            unescape_object_path_segment(parts[1].as_ref()) == Some(tenant.to_string()),
            "{name}"
        );
    }
}

fn rec(trace_id: [u8; 16], span_id: u8, parent: Option<u8>, start_ns: i64) -> SpanRecord {
    SpanRecord {
        tenant: "tenant-a".into(),
        span: span(trace_id, span_id, parent, start_ns),
    }
}

#[tokio::test]
async fn compact_block_keys_merges_late_spans_and_replaces_index_entries() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();

    let first = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([1; 16], 1, None, 100)],
        (10, 10),
    )
    .await
    .unwrap();
    let late = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([1; 16], 2, Some(1), 200)],
        (20, 20),
    )
    .await
    .unwrap();
    let (input_keys, output_key) = planned_job_keys("tenant-a", &[&first[0], &late[0]]);

    let meta = compact_block_keys(
        store.clone(),
        &writer,
        &mut index,
        "tenant-a",
        &input_keys,
        &output_key,
    )
    .await
    .unwrap();

    check!((meta.row_count, meta.min_ts, meta.max_ts) == (2, 100, 200));
    check!(
        meta.object_key
            .starts_with(output_key.trim_end_matches(".parquet"))
    );
    let output_key = meta.object_key;
    check!(
        index.candidate_blocks_for_trace("tenant-a", &[1; 16], 0, 1_000)
            == vec![output_key.clone()]
    );
    check!(
        index.prune_blocks_by_tag("tenant-a", "service.name", Some("api"), 0, 1_000)
            == vec![output_key.clone()]
    );

    let batches = read_block(store, &output_key).await.unwrap();
    assert2::assert!(
        batches
            .iter()
            .map(arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>()
            == 2
    );
}

#[tokio::test]
async fn compact_block_keys_recomputes_nested_sets_for_late_children() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();

    let first = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([1; 16], 1, None, 100)],
        (10, 10),
    )
    .await
    .unwrap();
    let late = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([1; 16], 2, Some(1), 200)],
        (20, 20),
    )
    .await
    .unwrap();
    let (input_keys, output_key) = planned_job_keys("tenant-a", &[&first[0], &late[0]]);

    let meta = compact_block_keys(
        store.clone(),
        &writer,
        &mut index,
        "tenant-a",
        &input_keys,
        &output_key,
    )
    .await
    .unwrap();

    let batches = read_block(store, &meta.object_key).await.unwrap();
    let batch = &batches[0];
    let span_ids = batch
        .column_by_name(krabka_blockstore::SCOL_SPAN_ID)
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let left = batch
        .column_by_name(krabka_blockstore::SCOL_NESTED_SET_LEFT)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let right = batch
        .column_by_name(krabka_blockstore::SCOL_NESTED_SET_RIGHT)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    let parent_id = batch
        .column_by_name(krabka_blockstore::SCOL_PARENT_ID)
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();

    let root = (0..batch.num_rows())
        .find(|row| span_ids.value(*row) == [1; 8])
        .unwrap();
    let child = (0..batch.num_rows())
        .find(|row| span_ids.value(*row) == [2; 8])
        .unwrap();

    check!(parent_id.value(child) == left.value(root));
    check!(left.value(root) < left.value(child));
    check!(right.value(child) < right.value(root));
}

fn rec_with_method(
    trace_id: [u8; 16],
    span_id: u8,
    parent: Option<u8>,
    start_ns: i64,
    method: &str,
) -> SpanRecord {
    let mut record = rec(trace_id, span_id, parent, start_ns);
    record.span.span_attrs = vec![KeyValue {
        key: "http.method".into(),
        value: AttrValue::Str(method.into()),
    }];
    record
}

/// Read a promoted string column, which the write path dictionary-encodes.
fn promoted_strings(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
    let dictionary = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("block has no `{column}` column"))
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("a dictionary column");
    let values = dictionary
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("string dictionary values");
    (0..dictionary.len())
        .map(|row| {
            (!dictionary.is_null(row)).then(|| {
                let key = usize::try_from(dictionary.keys().value(row)).expect("a dictionary key");
                values.value(key).to_string()
            })
        })
        .collect()
}

fn int64_values(batch: &RecordBatch, column: &str) -> Vec<i64> {
    batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("block has no `{column}` column"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("an i64 column")
        .values()
        .to_vec()
}

fn trace_ids(batch: &RecordBatch) -> Vec<Vec<u8>> {
    let ids = batch
        .column_by_name(SCOL_TRACE_ID)
        .expect("block has no trace id column")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("a fixed-size binary column");
    (0..ids.len()).map(|row| ids.value(row).to_vec()).collect()
}

/// Promoted attribute columns are an operator-configurable ingest feature, and
/// a block written with them has a schema strictly wider than the base one.
/// Compaction used to rebuild the base schema at this point, so the first
/// promoted block it read failed to concatenate and the tenant lost compaction
/// -- and with it the late-span merge -- without any signal.
#[tokio::test]
async fn compacting_promoted_blocks_keeps_the_promoted_column_and_its_values() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();
    let promoted = [PromotedSpanAttr::string("http.method")];

    let first = build_blocks_with_promoted_attrs(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec_with_method([1; 16], 1, None, 100, "GET")],
        (10, 10),
        &promoted,
    )
    .await
    .unwrap();
    let late = build_blocks_with_promoted_attrs(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec_with_method([1; 16], 2, Some(1), 200, "POST")],
        (20, 20),
        &promoted,
    )
    .await
    .unwrap();

    let (input_keys, output_key) = planned_job_keys("tenant-a", &[&first[0], &late[0]]);
    let meta = compact_block_keys(
        store.clone(),
        &writer,
        &mut index,
        "tenant-a",
        &input_keys,
        &output_key,
    )
    .await
    .expect("a promoted block compacts");

    let batches = read_block(store, &meta.object_key).await.unwrap();
    let batch = &batches[0];
    check!(int64_values(batch, SCOL_START_NANO) == vec![100, 200]);
    check!(
        promoted_strings(batch, "attr.http.method")
            == vec![Some("GET".to_string()), Some("POST".to_string())]
    );
}

/// An operator can add `--promote-span-attr` between two flushes, and the
/// compactor then sees inputs that disagree about the block schema. The output
/// carries the union of their columns: dropping the promoted column of the
/// half that has one would lose the dedicated column outright, and leaving it
/// null for the other half would claim the attribute is absent from rows that
/// carry it.
#[tokio::test]
async fn compacting_inputs_written_under_different_promotion_flags_fills_the_column() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();

    let before = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec_with_method([1; 16], 1, None, 100, "GET")],
        (10, 10),
    )
    .await
    .unwrap();
    let after = build_blocks_with_promoted_attrs(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec_with_method([1; 16], 2, Some(1), 200, "POST")],
        (20, 20),
        &[PromotedSpanAttr::string("http.method")],
    )
    .await
    .unwrap();

    let (input_keys, output_key) = planned_job_keys("tenant-a", &[&before[0], &after[0]]);
    let meta = compact_block_keys(
        store.clone(),
        &writer,
        &mut index,
        "tenant-a",
        &input_keys,
        &output_key,
    )
    .await
    .expect("mixed inputs compact");

    let batches = read_block(store, &meta.object_key).await.unwrap();
    let batch = &batches[0];
    check!(int64_values(batch, SCOL_START_NANO) == vec![100, 200]);
    check!(
        promoted_strings(batch, "attr.http.method")
            == vec![Some("GET".to_string()), Some("POST".to_string())],
        "the row from the unpromoted block is filled from its generic attributes"
    );
}

/// The span block declares `[trace_id, start_unix_nano]` as its sort key, and
/// the block's trace-id bounds are only usable for pruning if the rows are
/// really in that order. Concatenating inputs interleaves their traces, so the
/// compactor has to restore the order rather than inherit the read order.
#[tokio::test]
async fn a_compacted_block_is_ordered_by_trace_id_then_start() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = BlockWriter::new(store.clone());
    let mut index = TraceIndex::new();

    // Each input holds one span of each trace, so neither input's row order
    // nor their concatenation is the declared order.
    let first = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([2; 16], 1, None, 300), rec([1; 16], 2, None, 100)],
        (10, 10),
    )
    .await
    .unwrap();
    let second = build_blocks(
        &writer,
        &mut index,
        "tenant-a",
        7,
        &[rec([2; 16], 3, Some(1), 400), rec([1; 16], 4, Some(2), 200)],
        (20, 20),
    )
    .await
    .unwrap();

    let (input_keys, output_key) = planned_job_keys("tenant-a", &[&first[0], &second[0]]);
    let meta = compact_block_keys(
        store.clone(),
        &writer,
        &mut index,
        "tenant-a",
        &input_keys,
        &output_key,
    )
    .await
    .unwrap();

    let batches = read_block(store, &meta.object_key).await.unwrap();
    let batch = &batches[0];
    check!(
        trace_ids(batch) == vec![vec![1_u8; 16], vec![1; 16], vec![2; 16], vec![2; 16]],
        "rows are grouped by trace id, ascending"
    );
    check!(
        int64_values(batch, SCOL_START_NANO) == vec![100, 200, 300, 400],
        "and ordered by start within each trace"
    );
}
