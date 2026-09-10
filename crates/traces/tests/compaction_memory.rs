//! What a span-block compaction costs in memory.
//!
//! The claim the streaming compactor makes is that its working set is a
//! function of what it writes rather than of what it reads, and the only
//! honest way to check that is to measure memory. The workspace forbids
//! `unsafe`, so a counting global allocator is out; what is left, and what the
//! issue is actually about, is resident set size.
//!
//! RSS is a process-wide, monotonic number, so it is measured the only way it
//! can be measured cleanly: the parent builds the input blocks on disk and
//! then spawns a child process that does nothing but compact them. The child
//! reports `VmHWM - VmRSS`, taken across the compaction alone -- the peak the
//! process reached, less where it stood before the first block was opened.
//! Because the child never built the fixture, nothing it frees can be
//! re-used by the compaction and flatter the number.
//!
//! One arm is not enough to trust, because a test that only ever sees a flat
//! line cannot tell "memory does not grow with the input" from "this harness
//! cannot see memory grow". So the same child, over the same blocks, also runs
//! the buffered read the compactor used to do -- every input read whole and
//! concatenated -- and that arm is expected to double. The streaming arm is
//! only believable next to it.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

use arrow::compute::concat_batches;
use assert2::{assert, check};
use krabka_blockstore::{BlockWriter, TraceIndex, read_block};
use krabka_traces::{
    AttrValue, KeyValue, Span, SpanKind, SpanRecord, StatusCode, blockbuilder::build_blocks,
    compactor::compact_block_keys,
};
use object_store::{ObjectStore, local::LocalFileSystem};

/// Where the child finds the blocks it is to read.
const BLOCK_DIR: &str = "KRABKA_COMPACTION_MEMORY_DIR";
/// Which way the child is to read them: `stream` or `buffer`.
const ARM: &str = "KRABKA_COMPACTION_MEMORY_ARM";

/// Input blocks in each compaction. A k-way merge holds one decoded batch per
/// input, so the number of inputs is held fixed and their size is what varies
/// -- that is the axis the claim is about.
const BLOCKS: usize = 6;
/// Spans each block contributes to each trace. Every block carries the same
/// trace ids, so the merge really interleaves its inputs rather than reading
/// them end to end, and a trace's spans are spread across every block rather
/// than sitting whole in one.
const SPANS_PER_TRACE: u64 = 4;

/// One field of `/proc/self/status`, in kibibytes.
fn status_kib(field: &str) -> Option<u64> {
    let status = fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let rest = line.strip_prefix(field)?.strip_prefix(':')?;
        rest.trim().strip_suffix(" kB")?.trim().parse().ok()
    })
}

fn span(trace: u32, span_id: u64, start_ns: i64) -> Span {
    let mut trace_id = [0_u8; 16];
    trace_id[..4].copy_from_slice(&trace.to_be_bytes());
    Span {
        trace_id,
        span_id: span_id.to_be_bytes(),
        parent_span_id: (!span_id.is_multiple_of(4)).then(|| (span_id - span_id % 4).to_be_bytes()),
        name: format!("op-{}", span_id % 8),
        kind: SpanKind::Server,
        start_ns,
        duration_ns: 100,
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
        instrumentation_version: "1".into(),
    }
}

fn store_at(directory: &Path) -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new_with_prefix(directory).expect("a local object store"))
}

/// Writes [`BLOCKS`] span blocks of `traces` traces each under `directory`.
async fn build_blocks_under(directory: &Path, traces: u32) {
    let writer = BlockWriter::new(store_at(directory));
    let mut index = TraceIndex::new();
    for block in 0..BLOCKS {
        let offset = i64::try_from(block).expect("a small block index");
        let records = (0..traces)
            .flat_map(|trace| {
                (0..SPANS_PER_TRACE).map(move |within| SpanRecord {
                    tenant: "tenant".to_string(),
                    span: span(
                        trace,
                        u64::from(trace) * 64
                            + within
                            + u64::try_from(block).expect("a small block index") * 8,
                        offset * 1_000 + i64::try_from(within).expect("a small offset"),
                    ),
                })
            })
            .collect::<Vec<_>>();
        build_blocks(&writer, &mut index, "tenant", 0, &records, (offset, offset))
            .await
            .expect("the block is written");
    }
}

/// Every block key under `directory`, in a stable order.
fn block_keys(directory: &Path) -> Vec<String> {
    fn walk(at: &Path, root: &Path, into: &mut Vec<String>) {
        for entry in fs::read_dir(at)
            .expect("the directory is readable")
            .flatten()
        {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, root, into);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "parquet")
            {
                into.push(
                    path.strip_prefix(root)
                        .expect("the block is under the root")
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    let mut keys = Vec::new();
    walk(directory, directory, &mut keys);
    keys.sort();
    keys
}

/// The child: read the blocks the parent left, one arm or the other, and
/// report what it cost.
///
/// Ignored so it runs only when the parent spawns it with the environment it
/// needs.
#[tokio::test]
#[ignore = "spawned as a child process by the memory test"]
async fn compaction_memory_worker() {
    let directory = PathBuf::from(std::env::var(BLOCK_DIR).expect("the parent names a directory"));
    let arm = std::env::var(ARM).expect("the parent names an arm");
    let store = store_at(&directory);
    let keys = block_keys(&directory);

    let before = status_kib("VmRSS").expect("a Linux status file");
    match arm.as_str() {
        "stream" => {
            let mut index = TraceIndex::new();
            compact_block_keys(
                Arc::clone(&store),
                &BlockWriter::new(Arc::clone(&store)),
                &mut index,
                "tenant",
                &keys,
                "out.parquet",
            )
            .await
            .expect("the blocks compact");
        }
        // What the compactor used to do, and the control this test is read
        // against: every input read whole, then joined into one batch.
        "buffer" => {
            let mut batches = Vec::new();
            for key in &keys {
                batches.extend(
                    read_block(Arc::clone(&store), key)
                        .await
                        .expect("the block reads"),
                );
            }
            let schema = batches.first().expect("a block was read").schema();
            let joined = concat_batches(&schema, &batches).expect("the batches concatenate");
            println!("rows={}", joined.num_rows());
        }
        other => panic!("unknown arm `{other}`"),
    }
    let peak = status_kib("VmHWM").expect("a Linux status file");

    println!("growth-kib={}", peak.saturating_sub(before));
}

/// Runs the worker over `directory` and reports the kibibytes it grew by.
fn worker_growth_kib(directory: &Path, arm: &str) -> u64 {
    let output = Command::new(std::env::current_exe().expect("the test binary"))
        .args([
            "--exact",
            "compaction_memory_worker",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(BLOCK_DIR, directory)
        .env(ARM, arm)
        .output()
        .expect("the worker runs");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "the worker failed: {stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    // `--nocapture` prints the worker's line straight after the test name, so
    // the marker is looked for anywhere in the output rather than at the start
    // of a line.
    stdout
        .split("growth-kib=")
        .nth(1)
        .and_then(|rest| {
            rest.split(|byte: char| !byte.is_ascii_digit())
                .next()?
                .parse()
                .ok()
        })
        .unwrap_or_else(|| panic!("the worker reported no growth: {stdout}"))
}

/// Doubling what a compaction reads must not double what it costs.
///
/// The number of inputs is the same in both runs and only their size differs,
/// because that is the shape of the claim: a merge holds one decoded batch per
/// input whatever each input holds, so twice the rows behind the same number
/// of blocks should cost the same. The buffered arm is the control -- it is
/// the read the compactor used to do, over the same blocks in the same
/// process -- and its cost is expected to track the rows.
#[tokio::test(flavor = "multi_thread")]
async fn compacting_twice_as_many_rows_does_not_cost_twice_the_memory() {
    if status_kib("VmHWM").is_none() {
        // No /proc to read RSS out of. Nothing to measure rather than
        // something to fail.
        return;
    }

    let small = tempfile::tempdir().expect("a temporary directory");
    let large = tempfile::tempdir().expect("a temporary directory");
    build_blocks_under(small.path(), 5_000).await;
    build_blocks_under(large.path(), 10_000).await;

    let buffered = (
        worker_growth_kib(small.path(), "buffer"),
        worker_growth_kib(large.path(), "buffer"),
    );
    let streamed = (
        worker_growth_kib(small.path(), "stream"),
        worker_growth_kib(large.path(), "stream"),
    );

    // The fixed part of a child's growth -- the Parquet reader, the decoder's
    // own buffers, the code paged in on the way -- is the same in both arms
    // and at both sizes, so what the two sizes say about scaling is in the
    // difference between them rather than in the ratio of the totals.
    let buffered_marginal = buffered.1.saturating_sub(buffered.0);
    let streamed_marginal = streamed.1.saturating_sub(streamed.0);
    let report = format!(
        "read whole: {} KiB then {} KiB (marginal {buffered_marginal} KiB); \
         streamed: {} KiB then {} KiB (marginal {streamed_marginal} KiB)",
        buffered.0, buffered.1, streamed.0, streamed.1
    );
    println!("{report}");

    check!(
        buffered_marginal > 8 * 1_024,
        "the control must show the extra rows costing memory for the measurement to \
         mean anything -- {report}"
    );
    assert!(
        streamed_marginal * 4 < buffered_marginal,
        "twice the rows behind the same number of blocks should cost a streaming \
         compaction a fraction of what reading them whole costs -- {report}"
    );
}
