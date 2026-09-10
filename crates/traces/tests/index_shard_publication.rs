//! What a trace-index flush writes, and what a trace-index read fetches.
//!
//! The trace index used to be one object that every flush republished whole.
//! It is now a manifest over content-addressed shard payloads cut on a day
//! grid, so a flush writes the payloads of the shards its own blocks fall in
//! and names the rest by the keys the previous generation already used. These
//! tests pin that, and measure it.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use assert2::check;
use futures::stream::BoxStream;
use krabka_blockstore::{BlockLevel, ShardedTraceBloom, TraceBlockStats, TraceIndex};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
};

const INDEX_KEY: &str = "index/traces.json";
const TENANT: &str = "tenant-a";
const DAY_NS: i64 = 24 * 60 * 60 * 1_000_000_000;
const HOUR_NS: i64 = 60 * 60 * 1_000_000_000;
/// Blocks per day, one an hour, which is a slow tenant rather than a busy one.
const BLOCKS_PER_DAY: i64 = 24;
/// Days of blocks the seed publishes before anything is measured.
const RETENTION_DAYS: i64 = 30;
/// Trace ids in one block's bloom. A bloom is incompressible by construction
/// and is most of what a trace index weighs, so this is the number that decides
/// whether the measurement means anything.
const TRACES_PER_BLOCK: usize = 2_000;

/// Object store that records every put, so a test can say what a flush wrote
/// rather than what it hoped a flush wrote.
struct RecordingStore {
    inner: Arc<InMemory>,
    puts: std::sync::Mutex<Vec<(String, usize)>>,
    /// Distinct objects read since the last reset. Distinct rather than
    /// counted, because one capped read is a head and then a get and the test
    /// is about which objects a load touches, not how it touches them.
    reads: std::sync::Mutex<BTreeSet<String>>,
}

impl RecordingStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            puts: std::sync::Mutex::new(Vec::new()),
            reads: std::sync::Mutex::new(BTreeSet::new()),
        }
    }

    fn reset(&self) {
        self.puts.lock().expect("puts lock").clear();
        self.reads.lock().expect("reads lock").clear();
    }

    /// `(objects, bytes)` put since the last reset, counting only the objects
    /// whose key contains `part`.
    fn written(&self, part: &str) -> (usize, usize) {
        let puts = self.puts.lock().expect("puts lock");
        let matching = puts.iter().filter(|(key, _)| key.contains(part));
        (
            matching.clone().count(),
            matching.map(|(_, len)| *len).sum::<usize>(),
        )
    }

    /// Distinct objects read since the last reset.
    fn objects_read(&self) -> usize {
        self.reads.lock().expect("reads lock").len()
    }
}

impl std::fmt::Debug for RecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingStore")
    }
}

impl std::fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RecordingStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.puts
            .lock()
            .expect("puts lock")
            .push((location.to_string(), payload.content_length()));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.reads
            .lock()
            .expect("reads lock")
            .insert(location.to_string());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// A block record of the weight a real one has: a bloom over `TRACES_PER_BLOCK`
/// trace ids, and the tag sets a Tempo-shaped span block carries.
fn block(index: i64) -> TraceBlockStats {
    let min_ts = index * HOUR_NS;
    let mut bloom = ShardedTraceBloom::with_tempo_defaults(TRACES_PER_BLOCK);
    for trace in 0..TRACES_PER_BLOCK {
        let mut trace_id = [0_u8; 16];
        trace_id[..8].copy_from_slice(&index.to_le_bytes());
        trace_id[8..].copy_from_slice(&(trace as u64).to_le_bytes());
        bloom.insert(&trace_id);
    }
    let mut tag_names = BTreeSet::new();
    let mut tag_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (tag, value) in [
        ("service.name", "checkout"),
        ("span.kind", "server"),
        ("http.method", "POST"),
        ("deployment.environment", "prod"),
    ] {
        tag_names.insert(tag.to_string());
        tag_values
            .entry(tag.to_string())
            .or_default()
            .insert(value.to_string());
    }
    TraceBlockStats {
        object_key: format!("traces/{TENANT}/00000/{index:020}.parquet"),
        min_ts,
        max_ts: min_ts + HOUR_NS - 1,
        bloom,
        tag_names,
        tag_values,
        row_count: 10_000,
        level: BlockLevel::INGESTED,
    }
}

/// What the old layout wrote on every flush: the whole index, as one
/// `serde_json` document of the shape it had.
fn monolithic_snapshot_bytes(blocks: &[TraceBlockStats]) -> usize {
    serde_json::to_vec(&serde_json::json!({
        "tenants": { TENANT: { "blocks": blocks } }
    }))
    .expect("a trace index serialises")
    .len()
}

fn seeded() -> (TraceIndex, Vec<TraceBlockStats>) {
    let mut index = TraceIndex::new();
    let mut blocks = Vec::new();
    for hour in 0..(RETENTION_DAYS * BLOCKS_PER_DAY) {
        let stats = block(hour);
        index.add_trace_block(TENANT, stats.clone());
        blocks.push(stats);
    }
    (index, blocks)
}

/// The claim the layout exists to make: a flush stops republishing the world.
///
/// One block is added to a tenant that already holds a month of them, and the
/// write that publishes it is one shard payload and one manifest. Every other
/// shard is named by the key the previous generation gave it, so the bytes a
/// flush costs are the bytes of the day it touched, not of the retention.
#[tokio::test]
async fn a_flush_writes_one_shard_and_one_manifest_however_many_days_are_indexed() {
    let recorder = Arc::new(RecordingStore::new());
    let store: Arc<dyn ObjectStore> = Arc::clone(&recorder) as Arc<dyn ObjectStore>;
    let (mut index, mut blocks) = seeded();
    index.save_latest_snapshot(&store, INDEX_KEY).await.unwrap();

    recorder.reset();
    let next = block(RETENTION_DAYS * BLOCKS_PER_DAY);
    blocks.push(next.clone());
    index.add_trace_block(TENANT, next);
    index.save_latest_snapshot(&store, INDEX_KEY).await.unwrap();

    let (payload_objects, payload_bytes) = recorder.written("/payloads/");
    let (manifest_objects, manifest_bytes) = recorder.written("/snapshots/");
    let before = monolithic_snapshot_bytes(&blocks);

    let flush_bytes = payload_bytes + manifest_bytes;
    println!(
        "blocks={} flush_objects={} flush_bytes={flush_bytes} \
         (payload {payload_bytes} + manifest {manifest_bytes}) \
         monolithic_bytes={before} ratio={}x",
        blocks.len(),
        payload_objects + manifest_objects,
        before / flush_bytes.max(1),
    );

    check!(payload_objects == 1);
    check!(manifest_objects == 1);
    // The new block sits in a day of its own, so the shard it rewrote holds
    // only it. Every other day is carried by reference.
    check!(payload_bytes < before / 100);
}

/// The other half of the claim: a shard a merge read and put back unchanged is
/// not rewritten.
///
/// A compaction inside one day rewrites that day's shard and leaves the rest,
/// even though the merge had to fetch the shard it changed.
#[tokio::test]
async fn a_compaction_inside_one_day_rewrites_only_that_day() {
    let recorder = Arc::new(RecordingStore::new());
    let store: Arc<dyn ObjectStore> = Arc::clone(&recorder) as Arc<dyn ObjectStore>;
    let (index, _) = seeded();
    index.save_latest_snapshot(&store, INDEX_KEY).await.unwrap();

    let mut compactor = TraceIndex::load_latest_snapshot(&store, INDEX_KEY)
        .await
        .unwrap();
    let inputs = (0..BLOCKS_PER_DAY)
        .map(|hour| block(hour).object_key)
        .collect::<Vec<_>>();
    let mut compacted = block(0);
    compacted.object_key = format!("traces/{TENANT}/compacted/day-0.parquet");
    compacted.max_ts = BLOCKS_PER_DAY * HOUR_NS - 1;
    let level = compactor.replace_trace_blocks(TENANT, &inputs, compacted.clone());

    recorder.reset();
    compactor
        .save_latest_snapshot(&store, INDEX_KEY)
        .await
        .unwrap();

    let (payload_objects, _) = recorder.written("/payloads/");
    check!(level == BlockLevel::INGESTED.next());
    check!(payload_objects == 1);

    let reloaded = TraceIndex::load_latest_snapshot(&store, INDEX_KEY)
        .await
        .unwrap();
    let keys = reloaded
        .trace_blocks(TENANT)
        .iter()
        .map(|block| block.object_key.clone())
        .collect::<BTreeSet<_>>();
    check!(keys.len() == usize::try_from((RETENTION_DAYS - 1) * BLOCKS_PER_DAY + 1).unwrap());
    check!(keys.contains(&compacted.object_key));
    for input in &inputs {
        check!(!keys.contains(input));
    }
}

/// A reader that knows its time range fetches the payloads that meet it and no
/// others.
///
/// The span of a shard is in the manifest, so this is decided before any
/// payload is read: a query about one day costs one manifest and one payload,
/// whatever the retention is.
#[tokio::test]
async fn a_query_about_one_day_reads_one_days_shard() {
    let recorder = Arc::new(RecordingStore::new());
    let store: Arc<dyn ObjectStore> = Arc::clone(&recorder) as Arc<dyn ObjectStore>;
    let (index, _) = seeded();
    index.save_latest_snapshot(&store, INDEX_KEY).await.unwrap();

    recorder.reset();
    let whole = TraceIndex::load_latest_snapshot(&store, INDEX_KEY)
        .await
        .unwrap();
    let whole_reads = recorder.objects_read();

    recorder.reset();
    let day = 7;
    let scoped = TraceIndex::load_latest_snapshot_for_range_with_max_bytes(
        &store,
        INDEX_KEY,
        TENANT,
        day * DAY_NS,
        day * DAY_NS + DAY_NS - 1,
        krabka_blockstore::MAX_INDEX_SNAPSHOT_BYTES,
    )
    .await
    .unwrap();
    let scoped_reads = recorder.objects_read();

    println!("whole_index_objects_read={whole_reads} one_day_objects_read={scoped_reads}");

    // One manifest plus one payload, against one manifest plus a payload a day.
    check!(scoped_reads == 2);
    check!(whole_reads == usize::try_from(RETENTION_DAYS).unwrap() + 1);
    check!(
        scoped.trace_blocks(TENANT).len() == usize::try_from(BLOCKS_PER_DAY).unwrap(),
        "the day's blocks and nothing else"
    );
    check!(whole.trace_blocks(TENANT).len() > scoped.trace_blocks(TENANT).len());
    // The blocks it did keep are the same records the whole-index load has.
    let scoped_keys = scoped
        .trace_blocks(TENANT)
        .iter()
        .map(|block| block.object_key.clone())
        .collect::<BTreeSet<_>>();
    for hour in (day * BLOCKS_PER_DAY)..((day + 1) * BLOCKS_PER_DAY) {
        check!(scoped_keys.contains(&block(hour).object_key));
    }
}

/// A block whose span crosses a day boundary is written into both days.
///
/// That duplication is the mechanism: cutting shards on block boundaries
/// instead is what would make an append rewrite its neighbours. What it must
/// not do is reach a reader twice, because a block named twice is a block
/// scanned twice.
#[tokio::test]
async fn a_block_that_straddles_midnight_is_in_both_days_and_reaches_a_reader_once() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut index = TraceIndex::new();
    let mut straddling = block(0);
    straddling.object_key = format!("traces/{TENANT}/00000/straddling.parquet");
    straddling.min_ts = DAY_NS - HOUR_NS;
    straddling.max_ts = DAY_NS + HOUR_NS;
    index.add_trace_block(TENANT, straddling.clone());
    index.save_latest_snapshot(&store, INDEX_KEY).await.unwrap();

    let whole = TraceIndex::load_latest_snapshot(&store, INDEX_KEY)
        .await
        .unwrap();
    check!(
        whole
            .trace_blocks(TENANT)
            .iter()
            .filter(|block| block.object_key == straddling.object_key)
            .count()
            == 1,
        "one record, however many shards carry it"
    );

    for day in [0, 1] {
        let scoped = TraceIndex::load_latest_snapshot_for_range_with_max_bytes(
            &store,
            INDEX_KEY,
            TENANT,
            day * DAY_NS,
            day * DAY_NS + DAY_NS - 1,
            krabka_blockstore::MAX_INDEX_SNAPSHOT_BYTES,
        )
        .await
        .unwrap();

        let keys = scoped
            .trace_blocks(TENANT)
            .iter()
            .map(|block| block.object_key.clone())
            .collect::<Vec<_>>();
        check!(keys == vec![straddling.object_key.clone()], "day {day}");
    }
}
