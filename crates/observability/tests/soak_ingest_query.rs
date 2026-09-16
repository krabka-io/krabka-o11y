//! Ingest and query together, for a bounded wall clock, against a real object
//! store.
//!
//! The scale suite beside this one asks whether a large fixture still gives the
//! right answer. This asks a different question: whether the system stays the
//! same shape while it is used. Those fail differently. A leak, a cache with no
//! bound, a per-query allocation that is never freed, a retry loop that turns
//! one read into fifty -- none of them returns a wrong answer, and none of them
//! shows up in a run that does one thing once.
//!
//! # What it asserts, and what it refuses to assert
//!
//! It asserts on **resident memory growth** and on **object-store request
//! counts per operation**. It does not assert on wall time, and that is the
//! whole reason it can be a gate rather than a report. Wall time on a shared
//! runner is a statement about the runner. Both numbers here are ratios of
//! things the test itself counted, so a runner that gives this job half a core
//! makes the soak do fewer iterations, not fail.
//!
//! Request counts are the sharper of the two. An object store charges per
//! request, so "how many round trips does one query cost" is a number with a
//! bill attached, and it is exactly the number that grows when someone adds a
//! `head` before a `get`, or a manifest re-read inside a loop. The counting
//! store below sits between the block store and `MinIO` and counts every
//! call.
//!
//! Resident memory is the blunter one. An allocator returns pages to the OS
//! when it feels like it, so RSS after a burst of work is not the live heap and
//! a strict bound on it would fail for reasons that are not leaks. The shape
//! used here is a warm-up, then a measurement: RSS is sampled once the run has
//! done enough work to have allocated whatever it allocates per iteration, and
//! again at the end. What that catches is growth that does not stop, which is
//! what a leak looks like and what allocator retention does not.
//!
//! # Running
//!
//! ```text
//! bazel test --config=scale //crates/observability:soak_ingest_query_docker_test
//! ```
//!
//! `KRABKA_SOAK_SECONDS` sets the budget; it defaults to 60. A soak is more
//! useful the longer it runs, and a scheduled job still has to end, so the
//! budget is named rather than assumed. Nothing here waits on a fixed sleep,
//! so a longer budget buys more iterations rather than more idling.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use assert2::assert;
use datafusion::arrow::{
    array::{Float64Array, Int64Array, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use krabka_blockstore::{
    BlockWriter, COL_FINGERPRINT, COL_TIMESTAMP, Index, LabelMatcher, Labels, MatchOp,
    MeteredObjectStore, ObjectStoreMetrics, ObjectStoreOperation, read_block,
};
use object_store::{ObjectStore, aws::AmazonS3Builder};
use serde_json::json;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{ContainerPort, WaitFor},
    runners::AsyncRunner as _,
};
use tokio::sync::RwLock;

const TENANT: &str = "soak";
const BUCKET: &str = "krabka";
const MINIO_USER: &str = "krabkasoak";
const MINIO_PASSWORD: &str = "krabkasoak";
const MINIO_API_PORT: u16 = 9000;
const CONTAINER_START_TIMEOUT: Duration = Duration::from_mins(2);

/// The default budget, in seconds.
const DEFAULT_SOAK_SECONDS: u64 = 60;

/// Series per written block, and samples per series. Small enough that one
/// iteration is a fraction of a second, so a 60-second budget is hundreds of
/// iterations rather than three.
const SERIES: usize = 500;
const SAMPLES: usize = 4;

/// A block covers two hours.
const BLOCK_SPAN_MS: i64 = 2 * 60 * 60 * 1_000;

/// Distinct block keys the ingest side cycles through, and so the size of the
/// working set the soak holds steady. Sixty-four two-hour blocks is five days
/// of retention, which is a plausible window and a bounded one.
const RETAINED_BLOCKS: usize = 64;

/// The time range each query asks for: the first four slots. Four candidate
/// blocks per query rather than one, so a query is a small scan rather than a
/// point lookup, and the get-per-read ratio is measured over more than a single
/// read path.
const QUERY_WINDOW_MS: i64 = 4 * BLOCK_SPAN_MS - 1;

const LATENCY_BUCKETS_US: [u64; 16] = [
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    u64::MAX,
];

#[derive(Default)]
struct Latencies {
    counts: [u64; LATENCY_BUCKETS_US.len()],
    total: u64,
}

impl Latencies {
    fn observe(&mut self, elapsed: Duration) {
        let micros = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let bucket = LATENCY_BUCKETS_US
            .iter()
            .position(|upper| micros <= *upper)
            .unwrap_or(LATENCY_BUCKETS_US.len() - 1);
        self.counts[bucket] += 1;
        self.total += 1;
    }

    fn quantile(&self, numerator: u64, denominator: u64) -> u64 {
        let wanted = self.total.saturating_mul(numerator).div_ceil(denominator);
        let mut seen = 0;
        for (upper, count) in LATENCY_BUCKETS_US.iter().zip(self.counts) {
            seen += count;
            if seen >= wanted {
                return *upper;
            }
        }
        0
    }
}

// ---------------------------------------------------------------------------
// Resident memory.
// ---------------------------------------------------------------------------

/// This process's resident set size, in kibibytes.
///
/// Read from `/proc/self/status`, which is the only source that needs no crate
/// and no `unsafe`. The soak runs on Linux in CI and nowhere else, so a missing
/// `/proc` is a failure rather than a reason to skip: a soak that quietly
/// stopped measuring memory is the thing this whole file exists to prevent
/// elsewhere.
fn resident_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status")
        .expect("/proc/self/status is readable; this suite runs on Linux");
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kib = rest
                .split_whitespace()
                .next()
                .expect("VmRSS carries a number");
            return kib.parse().expect("VmRSS is a number of kibibytes");
        }
    }
    panic!("/proc/self/status carries no VmRSS line")
}

// ---------------------------------------------------------------------------
// Fixtures and the container.
// ---------------------------------------------------------------------------

fn series_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(COL_FINGERPRINT, DataType::UInt64, false),
        Field::new(COL_TIMESTAMP, DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]))
}

fn series_labels(which: usize) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", "http_requests_total");
    labels.insert("job", format!("job-{}", which % 16));
    labels.insert("pod", format!("pod-{which}"));
    labels
}

fn series_batch(fingerprints: &[u64], block: usize) -> RecordBatch {
    let start = i64::try_from(block).expect("a block index fits an i64") * BLOCK_SPAN_MS;
    let mut fps = Vec::with_capacity(fingerprints.len() * SAMPLES);
    let mut timestamps = Vec::with_capacity(fingerprints.len() * SAMPLES);
    let mut values = Vec::with_capacity(fingerprints.len() * SAMPLES);
    for fingerprint in fingerprints {
        for sample in 0..SAMPLES {
            fps.push(*fingerprint);
            let offset = i64::try_from(sample).expect("a sample index fits an i64");
            timestamps.push(start + offset * 1_000);
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

async fn start_minio() -> (ContainerAsync<GenericImage>, Arc<dyn ObjectStore>) {
    let tag = std::env::var("KRABKA_MINIO_IMAGE_TAG").expect(
        "KRABKA_MINIO_IMAGE_TAG is unset. This suite runs under `bazel test --config=scale`, \
         which loads the digest-pinned image and sets this. To run it under cargo, set it to \
         that image's tag in //bazel/images/images.bzl.",
    );

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
            // The bucket is a directory made before MinIO reads the data root;
            // see the same note in scale_object_store.rs.
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
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .expect("the S3 store is configured");

    (container, Arc::new(store))
}

// ---------------------------------------------------------------------------
// The soak.
// ---------------------------------------------------------------------------

/// Ingest and query concurrently for a bounded time, then read the counters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "soak: needs a MinIO container and a wall-clock budget"]
async fn ingest_and_query_together_hold_memory_and_request_count() {
    let budget = Duration::from_secs(
        std::env::var("KRABKA_SOAK_SECONDS")
            .ok()
            .and_then(|seconds| seconds.parse().ok())
            .unwrap_or(DEFAULT_SOAK_SECONDS),
    );

    let (_container, backing) = start_minio().await;
    let run_started = Instant::now();
    let object_store_metrics = ObjectStoreMetrics::unregistered();
    let store = MeteredObjectStore::wrap(backing, object_store_metrics.clone());

    let mut seed = Index::new();
    let mut fingerprints: Vec<u64> = (0..SERIES)
        .map(|which| {
            let labels = series_labels(which);
            let fingerprint = labels.fingerprint();
            seed.add_series(TENANT, fingerprint, &labels);
            fingerprint
        })
        .collect();
    fingerprints.sort_unstable();

    let index = Arc::new(RwLock::new(seed));
    let fingerprints = Arc::new(fingerprints);
    let deadline = Instant::now() + budget;
    let measure_after = Instant::now() + budget / 4;

    // The ingest side: write a block, then publish it to the index.
    let ingest = {
        let store = Arc::clone(&store);
        let index = Arc::clone(&index);
        let fingerprints = Arc::clone(&fingerprints);
        tokio::spawn(async move {
            let writer = BlockWriter::new(store);
            let mut written = 0_u64;
            while Instant::now() < deadline {
                // Blocks are written into a fixed ring of keys, and each one
                // replaces the block already in its slot. That is what makes
                // the working set bounded: the index holds at most
                // `RETAINED_BLOCKS` entries and the bucket at most that many
                // objects, however long the soak runs.
                //
                // The bound is the whole point of the memory assertion. An
                // ingest that only ever appends makes the index grow with the
                // run, so resident memory grows with the run too, and it grows
                // for a correct reason -- which leaves the assertion unable to
                // tell that from a leak, and unable to survive a longer budget.
                // A real deployment bounds the same thing by retention; this
                // bounds it by a ring, which needs no clock.
                let slot = usize::try_from(written).expect("an iteration count fits a usize")
                    % RETAINED_BLOCKS;
                let key = format!("soak/{TENANT}/{slot:04}.parquet");
                let meta = writer
                    .write_block(
                        TENANT,
                        &key,
                        series_schema(),
                        // The slot, not the iteration: a slot's block always
                        // covers the same window, so the query side's time
                        // range keeps selecting the same set of slots however
                        // many times they have been rewritten.
                        std::slice::from_ref(&series_batch(&fingerprints, slot)),
                    )
                    .await
                    .expect("the block writes");
                index.write().await.replace_blocks(
                    TENANT,
                    std::slice::from_ref(&key),
                    std::slice::from_ref(&meta),
                );
                written += 1;
                // Yield rather than sleep: the budget should buy iterations,
                // and a sleep would make the soak a measure of the clock.
                tokio::task::yield_now().await;
            }
            written
        })
    };

    // The query side: prune, then read whatever the prune named.
    let query = {
        let store = Arc::clone(&store);
        let index = Arc::clone(&index);
        tokio::spawn(async move {
            let matcher = [LabelMatcher::new("pod", MatchOp::Eq, "pod-42")];
            let mut served = 0_u64;
            let mut reads = 0_u64;
            let mut latencies = Latencies::default();
            while Instant::now() < deadline {
                let started = Instant::now();
                let candidates = {
                    let guard = index.read().await;
                    let Ok(selected) = guard.resolve(TENANT, &matcher) else {
                        continue;
                    };
                    guard.candidate_blocks(TENANT, &selected, 0, QUERY_WINDOW_MS)
                };
                if candidates.is_empty() {
                    // Nothing published yet. Not a query.
                    tokio::task::yield_now().await;
                    continue;
                }
                for key in &candidates {
                    let batches = read_block(Arc::clone(&store), key)
                        .await
                        .expect("a published block reads back");
                    assert!(!batches.is_empty());
                    reads += 1;
                }
                served += 1;
                if Instant::now() >= measure_after {
                    latencies.observe(started.elapsed());
                }
                tokio::task::yield_now().await;
            }
            (served, reads, latencies)
        })
    };

    // Warm-up: let both sides reach steady state before the memory sample that
    // the assertion is measured against. A sample taken at t=0 would include
    // every one-off allocation the runtime, the S3 client and the Parquet
    // writer make on their first call, and would report those as growth.
    tokio::time::sleep(budget / 4).await;
    let warm_kib = resident_kib();

    let written = ingest.await.expect("the ingest task did not panic");
    let (served, reads, latencies) = query.await.expect("the query task did not panic");
    let end_kib = resident_kib();

    let puts = object_store_metrics.operations(ObjectStoreOperation::Put)
        + object_store_metrics.operations(ObjectStoreOperation::PutMultipart);
    let gets = object_store_metrics.operations(ObjectStoreOperation::Get);
    let lists = object_store_metrics.operations(ObjectStoreOperation::List)
        + object_store_metrics.operations(ObjectStoreOperation::ListWithDelimiter);

    let seconds = budget.as_secs();
    println!(
        "soak: {seconds}s budget, {written} blocks written, {served} queries served, \
         {reads} block reads"
    );
    println!("soak: {puts} puts, {gets} gets, {lists} lists");
    println!(
        "soak: RSS {warm_kib} KiB after warm-up, {end_kib} KiB at the end ({:+} KiB)",
        i64::try_from(end_kib).expect("an RSS in KiB fits an i64")
            - i64::try_from(warm_kib).expect("an RSS in KiB fits an i64")
    );

    // The run has to have done something, or every ratio below divides by a
    // number the run never earned. This is the structural check: a soak that
    // idled reports clean ratios and means nothing.
    assert!(written >= 8);
    assert!(served >= 8);
    assert!(latencies.total > 0);

    // Requests per operation. A write of one block is one multipart upload or
    // one put; the block store may add a small constant around it. Four is
    // room for that constant and no room for a loop.
    let puts_per_block = puts / written;
    assert!(puts_per_block <= 4);

    // A read of one block is a metadata fetch and a data fetch, plus the `head`
    // the reader does first to check the size cap. Six is room for those and
    // for one retry; it is not room for a per-row or per-row-group round trip,
    // which is the failure this number exists to catch.
    let gets_per_read = gets / reads;
    assert!(gets_per_read <= 6);

    let transferred_bytes = ObjectStoreOperation::all()
        .into_iter()
        .map(|operation| object_store_metrics.transferred_bytes(operation))
        .sum::<u64>();
    let report = json!({
        "schema_version": 1,
        "commit": std::env::var("KRABKA_SOAK_COMMIT").unwrap_or_else(|_| "unknown".into()),
        "signal": "metrics",
        "object_store": {
            "provider": "minio-s3",
            "image": std::env::var("KRABKA_MINIO_IMAGE_TAG").unwrap_or_else(|_| "unknown".into()),
            "image_id": std::env::var("KRABKA_MINIO_IMAGE_ID").unwrap_or_else(|_| "unknown".into()),
            "requests": {"put": puts, "get": gets, "list": lists},
            "transferred_bytes": transferred_bytes,
        },
        "budget_seconds": seconds,
        "warm_up_seconds": (budget / 4).as_secs_f64(),
        "duration_seconds": run_started.elapsed().as_secs_f64(),
        "blocks_written": written,
        "ingest_blocks_per_second": written as f64 / budget.as_secs_f64(),
        "queries_served": served,
        "query_latency_us": {
            "p50": latencies.quantile(50, 100),
            "p95": latencies.quantile(95, 100),
            "p99": latencies.quantile(99, 100),
        },
        "errors": 0,
        "rss_kib": {"after_warm_up": warm_kib, "end": end_kib},
        "wal_lag": null,
    });
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
    if let Some(output_dir) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        std::fs::write(
            std::path::Path::new(&output_dir).join("soak-report.json"),
            serde_json::to_vec_pretty(&report).unwrap(),
        )
        .expect("the soak report writes");
    }

    // Pruning is an in-memory operation. It must not reach the object store at
    // all, and a `list` per query is how a tenant's bill grows without anyone
    // changing a query.
    assert!(lists == 0);

    // Resident memory after the warm-up must not keep climbing. The working set
    // is bounded by `RETAINED_BLOCKS`, so once the ring has been filled once --
    // which the warm-up covers many times over -- a correct run allocates and
    // frees the same shapes forever and RSS is flat apart from allocator
    // hysteresis. Growth past this bound is growth per iteration, which is what
    // a leak is.
    //
    // The bound is still loose. `1.5x` plus 64 MiB is room for an allocator
    // that returns pages late and for a runtime that grows its thread-local
    // caches under load; it is not room for anything proportional to the number
    // of iterations, which is the only thing this needs to catch. A tighter
    // bound would be measuring the allocator.
    let ceiling = warm_kib * 3 / 2 + 64 * 1024;
    assert!(end_kib <= ceiling);
}
