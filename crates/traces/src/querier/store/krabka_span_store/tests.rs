use std::sync::atomic::{AtomicU64, Ordering};

use assert2::check;
use krabka_blockstore::{BlockLevel, ShardedTraceBloom, TraceBlockStats, encode_span_rows};
use object_store::{ObjectStore, memory::InMemory, path::Path};

use super::*;
use crate::querier::{
    store::test_support::{RowGroupBlock, write_row_group_block},
    test_rows::api_root_server_row,
};

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    fetched: AtomicU64,
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "CountingStore({})", self.inner)
    }
}

krabka_blockstore::delegate_object_store! {
    CountingStore => inner;
    forward [put_opts, put_multipart_opts, delete_stream, list, list_with_delimiter, copy_opts];

    async fn get_opts(
        &self,
        location: &Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let head = options.head;
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            self.fetched.fetch_add(result.range.end - result.range.start, Ordering::Relaxed);
        }
        Ok(result)
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<bytes::Bytes>> {
        let result = self.inner.get_ranges(location, ranges).await?;
        self.fetched.fetch_add(
            result.iter().map(|bytes| u64::try_from(bytes.len()).unwrap()).sum(),
            Ordering::Relaxed,
        );
        Ok(result)
    }
}

async fn trace_lookup_fixture(target: &[u8; 16]) -> (Arc<CountingStore>, SharedTraceIndex) {
    let objects = Arc::new(InMemory::new());
    let mut index = TraceIndex::new();
    for (part, key) in [
        "blocks/first.parquet",
        "blocks/second.parquet",
        "blocks/false.parquet",
    ]
    .into_iter()
    .enumerate()
    {
        let mut rows = (0_u8..128)
            .filter(|id| *id != 42)
            .map(|id| {
                let mut row = api_root_server_row([id; 16], [id; 8]);
                row.name = Some(format!("unrelated-{id}-{}", "payload".repeat(100)));
                row
            })
            .collect::<Vec<_>>();
        if part < 2 {
            let mut row = api_root_server_row(*target, [u8::try_from(part).unwrap(); 8]);
            // Both fragments are outside the narrow hint. The block overlaps it.
            row.start_unix_nano = if part == 0 { 100 } else { 9_000 };
            rows.push(row);
        }
        let batch = encode_span_rows(&rows).unwrap();
        write_row_group_block(
            &objects,
            RowGroupBlock {
                key,
                schema: batch.schema(),
                batches: &[batch],
            },
        )
        .await;
        let mut bloom = ShardedTraceBloom::with_tempo_defaults(rows.len());
        // The last block is deliberately a Bloom false positive.
        bloom.insert(target);
        index.add_trace_block(
            "tenant",
            TraceBlockStats {
                object_key: key.into(),
                min_ts: 100,
                max_ts: 9_000,
                bloom,
                tag_names: BTreeSet::new(),
                tag_values: BTreeMap::new(),
                row_count: rows.len(),
                level: BlockLevel::INGESTED,
            },
        );
    }
    let counted = Arc::new(CountingStore {
        inner: objects,
        fetched: AtomicU64::new(0),
    });
    let index = Arc::new(ArcSwap::from_pointee(index));
    (counted, index)
}

#[tokio::test]
async fn trace_lookup_prunes_unrelated_row_groups_and_false_positive_blocks() {
    let target = [42; 16];
    let (counted, index) = trace_lookup_fixture(&target).await;
    let make_store = || {
        KrabkaSpanStore::new(
            Arc::new(BlockStore::new(
                counted.clone(),
                url::Url::parse("memory:///").unwrap(),
            )),
            Arc::clone(&index),
            None,
        )
    };
    let candidates =
        |index: &TraceIndex| index.candidate_blocks_for_trace("tenant", &target, 1_000, 1_000);
    let full = make_store()
        .indexed_batches(candidates, None)
        .await
        .unwrap();
    let full_bytes = counted.fetched.swap(0, Ordering::Relaxed);
    let filtered = make_store()
        .indexed_batches(candidates, Some(trace_id_filter(&target)))
        .await
        .unwrap();
    let filtered_bytes = counted.fetched.swap(0, Ordering::Relaxed);
    let rows = |batches: &[RecordBatch]| batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let memory = |batches: &[RecordBatch]| {
        batches
            .iter()
            .map(RecordBatch::get_array_memory_size)
            .sum::<usize>()
    };
    check!(rows(&full) == 383);
    check!(rows(&filtered) == 2);
    check!(filtered_bytes < full_bytes);
    let full_memory = memory(&full);
    let filtered_memory = memory(&filtered);
    check!(filtered_memory < full_memory);
    let expected = trace_from_batches(&target, full).unwrap();
    check!(trace_from_batches(&target, filtered).unwrap() == expected);
    check!(
        make_store()
            .trace_by_id_inner("tenant", &target, 1_000, 1_000)
            .await
            .unwrap()
            == expected
    );
    let lookup_bytes = counted.fetched.swap(0, Ordering::Relaxed);
    check!(lookup_bytes < full_bytes);
    check!(
        make_store()
            .trace_by_id_inner("tenant", &[255; 16], 1_000, 1_000)
            .await
            .unwrap()
            .is_none()
    );
    eprintln!(
        "full: {full_bytes} fetched bytes, 383 rows, {full_memory} materialized bytes; \
         filtered: {filtered_bytes} fetched bytes, 2 rows, {filtered_memory} materialized bytes"
    );
}
