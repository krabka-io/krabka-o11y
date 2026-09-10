//! The block store at the cardinality and volume the design targets, against a
//! real object store.
//!
//! Every other suite in this repository runs below the scale where this design
//! has anything to prove. The differential suites seed hundreds of series to
//! check that an answer matches an upstream, which is what they are for. The
//! unit tests write two rows. Nothing has ever written ten thousand series,
//! or a million spans, or compacted more than a handful of blocks, and nothing
//! has ever done any of it against an object store that answers over HTTP.
//!
//! That gap matters here more than it would elsewhere. A columnar block store
//! over object storage gives its benefit at cardinality and volume, and its failure
//! modes are invisible below them: an index whose postings stop pruning, a
//! compaction that reads more than it writes, a request pattern that turns one
//! query into thousands of round trips. None of those returns a wrong answer.
//! Every suite here would stay green through all of them.
//!
//! These are tests, not benchmarks, and they assert on shape rather than on
//! wall clock: how many blocks a prune selected, how many objects a compaction
//! left, whether every row survived. Wall-clock assertions would make the suite
//! a report on the runner's load. The benchmarks under //benches measure time;
//! this measures that the thing still works when the numbers get large.
//!
//! # Running
//!
//! ```text
//! bazel test --config=scale //crates/observability:scale_object_store_scale_test
//! ```
//!
//! Under Cargo, with a `MinIO` reachable and its tag named:
//!
//! ```text
//! KRABKA_MINIO_IMAGE_TAG=latest \
//!   cargo test -p krabka-observability --test scale_object_store -- --ignored --nocapture
//! ```
//!
//! Every test carries `#[ignore]`, so `cargo test` skips them the way it skips
//! the container suites. `[[test]] test = false` in Cargo.toml keeps the
//! `cargo` job from building this binary at all, and the `scale` tag in
//! BUILD.bazel keeps `bazel test //...` from building or running it. It runs on
//! a schedule and nowhere else.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use assert2::assert;
use datafusion::arrow::{
    array::{Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use krabka_blockstore::{
    AttrValue, BlockMeta, BlockWriter, COL_FINGERPRINT, COL_TIMESTAMP, Index, LabelMatcher, Labels,
    MatchOp, ShardedTraceBloom, SpanAttr, SpanKind, SpanNode, SpanRow, StatusCode, SummaryColumns,
    TraceBlockStats, TraceIndex, assign_nested_set, encode_span_rows, read_block, span_block_decl,
    span_block_schema,
};
use krabka_units::prelude::*;
use object_store::{
    ObjectStore, ObjectStoreExt as _, aws::AmazonS3Builder, path::Path as ObjectPath,
};
use testcontainers::{
    GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner as _,
};

/// The tenant every test here writes under.
const TENANT: &str = "scale";

/// The bucket the container is started with.
const BUCKET: &str = "krabka";

const MINIO_USER: &str = "krabkascale";
const MINIO_PASSWORD: &str = "krabkascale";
const MINIO_API_PORT: u16 = 9000;

/// A block covers two hours, as Prometheus cuts them.
const BLOCK_SPAN_MS: i64 = 2 * 60 * 60 * 1_000;

/// Starting a container and waiting for its API is not the thing under test, so
/// it gets a bound of its own rather than the suite's.
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

// ---------------------------------------------------------------------------
// The container, and the store over it.
// ---------------------------------------------------------------------------

/// A running `MinIO`, and an `ObjectStore` pointed at its bucket.
///
/// The container is returned alongside the store because testcontainers stops
/// it when the handle drops, and a store outliving its container is a test that
/// fails for a reason that has nothing to do with the code.
struct Minio {
    _container: testcontainers::ContainerAsync<GenericImage>,
    store: Arc<dyn ObjectStore>,
}

async fn start_minio() -> Minio {
    // No default. //bazel/defs.bzl sets this from //bazel/images/images.bzl,
    // the same map that decides what `docker load` tags. A default here would
    // be a second copy of that decision, and when the two disagree
    // testcontainers pulls the image over the network instead of using the
    // pinned bytes.
    let tag = std::env::var("KRABKA_MINIO_IMAGE_TAG").expect(
        "KRABKA_MINIO_IMAGE_TAG is unset. This suite runs under `bazel test --config=scale`, \
         which loads the digest-pinned image and sets this. To run it under cargo, set it to \
         that image's tag in //bazel/images/images.bzl.",
    );

    // The bucket is a directory under the data root, made before MinIO reads
    // it. MinIO has no "create this bucket at startup" switch, `object_store`
    // has no bucket-creation call, and creating one over the API needs a
    // SigV4-signed `PUT /<bucket>` that nothing in this dependency set can
    // build. Overriding the entrypoint is the one step that needs no extra
    // tool in the image and no extra crate in the manifest.
    let container = tokio::time::timeout(
        CONTAINER_START_TIMEOUT,
        GenericImage::new("mirror.gcr.io/minio/minio".to_string(), tag)
            .with_exposed_port(ContainerPort::Tcp(MINIO_API_PORT))
            // MinIO writes its whole banner to stderr, the `API:` line
            // included. Waiting on stdout waits for a stream that stays empty,
            // and testcontainers reports that as `WaitContainer(StartupTimeout)`
            // -- which reads like a container that failed to start, while the
            // container is up and serving.
            .with_wait_for(WaitFor::message_on_stderr("API:"))
            .with_entrypoint("/bin/sh")
            .with_env_var("MINIO_ROOT_USER", MINIO_USER)
            .with_env_var("MINIO_ROOT_PASSWORD", MINIO_PASSWORD)
            .with_cmd([
                "-c",
                &format!("mkdir -p /data/{BUCKET} && exec /usr/bin/minio server /data"),
            ])
            .start(),
    )
    .await
    .expect("MinIO started inside its timeout")
    .expect("MinIO started");

    let port = container
        .get_host_port_ipv4(MINIO_API_PORT)
        .await
        .expect("the API port is mapped");

    let store = AmazonS3Builder::new()
        .with_endpoint(format!("http://127.0.0.1:{port}"))
        .with_bucket_name(BUCKET)
        .with_access_key_id(MINIO_USER)
        .with_secret_access_key(MINIO_PASSWORD)
        .with_region("us-east-1")
        // MinIO speaks S3 over plain HTTP here, and the default rejects that.
        .with_allow_http(true)
        // Path style, because `http://127.0.0.1:port/krabka/...` has no
        // hostname to put a bucket in front of.
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("the S3 store is configured");

    Minio {
        _container: container,
        store: Arc::new(store),
    }
}

/// How many objects the store holds under `prefix`.
async fn object_count(store: &Arc<dyn ObjectStore>, prefix: &str) -> usize {
    use futures_util::TryStreamExt as _;
    store
        .list(Some(&ObjectPath::from(prefix)))
        .try_collect::<Vec<_>>()
        .await
        .expect("the prefix lists")
        .len()
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

/// The Arrow schema a metrics block carries: the two mandatory columns and a
/// value.
fn series_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

/// The label set of series `which`, with one high-cardinality label so that a
/// `pod` matcher selects exactly one series however many there are.
fn series_labels(which: usize) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", "http_requests_total");
    labels.insert("job", format!("job-{}", which % 16));
    labels.insert("instance", format!("instance-{}", which % 64));
    labels.insert("pod", format!("pod-{which}"));
    labels
}

/// One block's rows: every series, sampled `samples` times inside the block's
/// own window, sorted by fingerprint and then timestamp.
fn series_batch(fingerprints: &[u64], block: usize, samples: usize) -> RecordBatch {
    let start = i64::try_from(block).expect("a block index fits an i64") * BLOCK_SPAN_MS;
    let step = BLOCK_SPAN_MS / i64::try_from(samples).expect("a sample count fits an i64");

    let mut fps = Vec::with_capacity(fingerprints.len() * samples);
    let mut timestamps = Vec::with_capacity(fingerprints.len() * samples);
    let mut values = Vec::with_capacity(fingerprints.len() * samples);
    for fingerprint in fingerprints {
        for sample in 0..samples {
            fps.push(*fingerprint);
            timestamps
                .push(start + i64::try_from(sample).expect("a sample index fits an i64") * step);
            values.push(f64::from(
                u32::try_from(sample).expect("a sample index fits a u32"),
            ));
        }
    }

    RecordBatch::try_new(
        series_schema(),
        vec![
            Arc::new(UInt64Array::from(fps)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(values)),
        ],
    )
    .expect("the generated columns match the schema")
}

/// The span rows of one trace, as a flat root-and-children fan.
fn trace_rows(trace_id: [u8; 16], spans: usize, start_nano: i64) -> Vec<SpanRow> {
    let nodes: Vec<SpanNode> = (0..spans)
        .map(|index| SpanNode {
            span_id: span_id(index),
            parent_span_id: (index > 0).then(|| span_id(0)),
        })
        .collect();
    let nested = assign_nested_set(&nodes);

    nodes
        .iter()
        .zip(&nested)
        .enumerate()
        .map(|(index, (node, nset))| SpanRow {
            trace_id,
            span_id: node.span_id,
            parent_span_id: node.parent_span_id,
            nested_set: *nset,
            child_count: 0,
            root_service_name: Some(format!("svc-{}", index % 32)),
            root_span_name: Some("GET /api".into()),
            trace_start_unix_nano: start_nano,
            trace_duration: nanos(1_000),
            name: Some(format!("span-{index}")),
            kind: SpanKind::Server,
            start_unix_nano: start_nano + i64::try_from(index).expect("a span index fits an i64"),
            duration: nanos(10),
            status_code: StatusCode::Ok,
            status_message: None,
            instrumentation_name: Some("tracer".into()),
            instrumentation_version: None,
            attrs: vec![SpanAttr {
                key: "service.name".into(),
                is_array: false,
                value: AttrValue::Str(vec![format!("svc-{}", index % 32)]),
            }],
            events: vec![],
            links: vec![],
        })
        .collect()
}

fn span_id(index: usize) -> [u8; 8] {
    (u64::try_from(index).expect("a span index fits a u64") + 1).to_be_bytes()
}

fn trace_id_bytes(trace: usize) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id[..8].copy_from_slice(&0x7ACE_u64.to_be_bytes());
    id[8..].copy_from_slice(
        &u64::try_from(trace)
            .expect("a trace index fits a u64")
            .to_be_bytes(),
    );
    id
}

// ---------------------------------------------------------------------------
// The tests.
// ---------------------------------------------------------------------------

/// Ten thousand series over twelve blocks, then a prune that must select one.
///
/// The assertion that matters is the block count, not the row count. An index
/// that has stopped pruning still returns every series the matcher asks for --
/// it just reads every block to do it, and no correctness test anywhere would
/// notice. Here it is the difference between one candidate block and twelve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scale suite: needs a MinIO container and minutes of ingest"]
async fn ten_thousand_series_over_twelve_blocks_prune_to_one() {
    const SERIES: usize = 10_000;
    const BLOCKS: usize = 12;
    const SAMPLES: usize = 8;

    let minio = start_minio().await;
    let writer = BlockWriter::new(Arc::clone(&minio.store));
    let mut index = Index::new();

    let fingerprints: Vec<u64> = (0..SERIES)
        .map(|which| {
            let labels = series_labels(which);
            let fingerprint = labels.fingerprint();
            index.add_series(TENANT, fingerprint, &labels);
            fingerprint
        })
        .collect();
    let mut sorted = fingerprints.clone();
    sorted.sort_unstable();

    for block in 0..BLOCKS {
        let key = format!("blocks/{TENANT}/{block:06}.parquet");
        let meta = writer
            .write_block(
                TENANT,
                &key,
                series_schema(),
                std::slice::from_ref(&series_batch(&sorted, block, SAMPLES)),
            )
            .await
            .expect("the block writes to the object store");
        assert!(meta.row_count == SERIES * SAMPLES);
        index.add_block(&meta);
    }

    assert!(object_count(&minio.store, &format!("blocks/{TENANT}")).await == BLOCKS);
    assert!(index.block_count(TENANT) == BLOCKS);

    // One series out of ten thousand.
    let selected = index
        .resolve(TENANT, &[LabelMatcher::new("pod", MatchOp::Eq, "pod-4242")])
        .expect("the matcher selects a series");
    assert!(selected.len() == 1);

    // One block out of twelve: the range covers the first block's window only.
    let candidates = index.candidate_blocks(TENANT, &selected, 0, BLOCK_SPAN_MS - 1);
    assert!(candidates.len() == 1);

    // And the block it named does hold that series.
    let batches = read_block(Arc::clone(&minio.store), &candidates[0])
        .await
        .expect("the candidate block reads back");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert!(rows == SERIES * SAMPLES);

    // The label metadata over ten thousand series, which the `/api/v1/labels`
    // path reads in full rather than through a selection.
    assert!(index.label_values(TENANT, "job").len() == 16);
    assert!(index.label_values(TENANT, "pod").len() == SERIES);
}

/// A million spans, then a by-id lookup that must not read every block.
///
/// The by-id path has no posting list. It has a bloom filter per block, and a
/// bloom sized for the wrong number of items degrades into "maybe" for every
/// query -- which is correct, and reads the whole retention window to answer
/// one trace id. A million spans is where that shows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scale suite: needs a MinIO container and minutes of ingest"]
async fn one_million_spans_locate_by_trace_id_without_reading_every_block() {
    const BLOCKS: usize = 10;
    const TRACES_PER_BLOCK: usize = 5_000;
    const SPANS_PER_TRACE: usize = 20;

    let minio = start_minio().await;
    let writer = BlockWriter::new(Arc::clone(&minio.store));
    let mut index = TraceIndex::new();
    let mut written = 0_usize;

    for block in 0..BLOCKS {
        let start = i64::try_from(block).expect("a block index fits an i64") * 1_000_000_000;
        let mut rows = Vec::with_capacity(TRACES_PER_BLOCK * SPANS_PER_TRACE);
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(TRACES_PER_BLOCK);
        let mut services: BTreeSet<String> = BTreeSet::new();

        for trace in 0..TRACES_PER_BLOCK {
            let id = trace_id_bytes(block * TRACES_PER_BLOCK + trace);
            bloom.insert(&id);
            rows.extend(trace_rows(
                id,
                SPANS_PER_TRACE,
                start + i64::try_from(trace).expect("a trace index fits an i64"),
            ));
        }
        for index in 0..32 {
            services.insert(format!("svc-{index}"));
        }
        written += rows.len();

        let key = format!("traces/{TENANT}/{block:06}.parquet");
        let batch = encode_span_rows(&rows).expect("the span rows encode");
        let meta = writer
            .write_block_with_decl(
                TENANT,
                &key,
                span_block_schema(),
                std::slice::from_ref(&batch),
                &span_block_decl(),
                SummaryColumns::new("trace_id", "start_unix_nano"),
            )
            .await
            .expect("the span block writes to the object store");

        let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        tag_values.insert("service.name".to_string(), services);
        index.add_trace_block(
            TENANT,
            TraceBlockStats {
                object_key: key,
                min_ts: start,
                max_ts: start + 1_000_000_000 - 1,
                bloom,
                tag_names: tag_values.keys().cloned().collect(),
                tag_values,
                row_count: meta.row_count,
                level: meta.level,
            },
        );
    }

    assert!(written == BLOCKS * TRACES_PER_BLOCK * SPANS_PER_TRACE);
    assert!(written >= 1_000_000);
    assert!(object_count(&minio.store, &format!("traces/{TENANT}")).await == BLOCKS);

    // A trace that exists in block 3, looked up across the whole window. The
    // bloom must reject the other nine blocks: `<= 2` allows one false
    // positive, which a 1% filter is entitled to and a broken one is not.
    let wanted = trace_id_bytes(3 * TRACES_PER_BLOCK + 17);
    let candidates = index.candidate_blocks_for_trace(TENANT, &wanted, 0, i64::MAX);
    assert!(!candidates.is_empty());
    assert!(candidates.len() <= 2);

    // A trace that exists nowhere. Every block should reject it.
    let absent = trace_id_bytes(usize::MAX / 2);
    let none = index.candidate_blocks_for_trace(TENANT, &absent, 0, i64::MAX);
    assert!(none.len() <= 1);

    let batches = read_block(Arc::clone(&minio.store), &candidates[0])
        .await
        .expect("the candidate block reads back");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert!(rows == TRACES_PER_BLOCK * SPANS_PER_TRACE);
}

/// Twenty-four blocks compacted into one, with every row kept.
///
/// Compaction is what makes a retention window affordable, or fails to. The two things that go wrong are silent:
/// rows are dropped, or the old objects are left behind so the store grows
/// while the index says it shrank. Both are checked here, and neither is
/// visible in a test that compacts two blocks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scale suite: needs a MinIO container and minutes of ingest"]
async fn compaction_over_twenty_four_blocks_keeps_every_row() {
    const SERIES: usize = 2_000;
    const BLOCKS: usize = 24;
    const SAMPLES: usize = 8;

    let minio = start_minio().await;
    let writer = BlockWriter::new(Arc::clone(&minio.store));
    let mut index = Index::new();

    let mut fingerprints: Vec<u64> = (0..SERIES)
        .map(|which| {
            let labels = series_labels(which);
            let fingerprint = labels.fingerprint();
            index.add_series(TENANT, fingerprint, &labels);
            fingerprint
        })
        .collect();
    fingerprints.sort_unstable();

    let mut source_keys = Vec::with_capacity(BLOCKS);
    for block in 0..BLOCKS {
        let key = format!("compact/{TENANT}/src-{block:06}.parquet");
        let meta = writer
            .write_block(
                TENANT,
                &key,
                series_schema(),
                std::slice::from_ref(&series_batch(&fingerprints, block, SAMPLES)),
            )
            .await
            .expect("the source block writes");
        index.add_block(&meta);
        source_keys.push(key);
    }

    let before: usize = index
        .all_blocks(TENANT)
        .iter()
        .map(|meta| meta.row_count)
        .sum();
    assert!(before == SERIES * SAMPLES * BLOCKS);

    // Read every source block back and write the union as one. This is the
    // shape a compactor has: read N, merge, write one, swap the index, delete
    // the sources.
    let mut merged: Vec<RecordBatch> = Vec::with_capacity(BLOCKS);
    for key in &source_keys {
        merged.extend(
            read_block(Arc::clone(&minio.store), key)
                .await
                .expect("a source block reads back"),
        );
    }
    let merged_rows: usize = merged.iter().map(RecordBatch::num_rows).sum();
    assert!(merged_rows == before);

    let compacted_key = format!("compact/{TENANT}/merged.parquet");
    let compacted: BlockMeta = writer
        .write_block(TENANT, &compacted_key, series_schema(), &merged)
        .await
        .expect("the compacted block writes");
    assert!(compacted.row_count == before);

    index.replace_blocks(TENANT, &source_keys, std::slice::from_ref(&compacted));
    assert!(index.block_count(TENANT) == 1);

    // The index now names one block, and the store must too. An index that
    // swapped without the delete is the case where the bill keeps rising while
    // the metrics say the store shrank.
    for key in &source_keys {
        minio
            .store
            .delete(&ObjectPath::from(key.as_str()))
            .await
            .expect("a source object deletes");
    }
    assert!(object_count(&minio.store, &format!("compact/{TENANT}")).await == 1);

    let back = read_block(Arc::clone(&minio.store), &compacted_key)
        .await
        .expect("the compacted block reads back");
    let after: usize = back.iter().map(RecordBatch::num_rows).sum();
    assert!(after == before);
}
