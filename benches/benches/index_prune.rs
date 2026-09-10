//! Pruning: turning matchers and a time range into the blocks worth reading.
//!
//! This is the operation the block-store design depends on most. Everything
//! downstream is proportional to what pruning hands it, so a pruner that
//! degrades turns a cheap query into a full scan without returning a wrong
//! answer anywhere. Nothing else in this repository would notice.
//!
//! The sweep is over series count, up to a hundred thousand. Two of the cases
//! are chosen to have different expected curves: resolving a `pod` matcher
//! selects exactly one series however many there are, and resolving a `job`
//! matcher selects a sixteenth of them. The first should be flat and the
//! second linear. A run where the first has become linear too is an index that
//! has stopped being an index.

use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_blockstore::{Index, LabelMatcher, MatchOp};
use krabka_o11y_benches::index::{BLOCK_SPAN_MS, TENANT, populated_index, populated_trace_index};
use object_store::{ObjectStore, memory::InMemory};

/// Series counts to sweep. A hundred thousand is the order of magnitude a
/// single Prometheus replica carries, and nothing in this workspace had ever
/// been run against it.
const SERIES: [usize; 3] = [1_000, 10_000, 100_000];

/// Blocks in the index while the series sweep runs. Twelve two-hour blocks is
/// a day of retention.
const BLOCKS: usize = 12;

/// Block counts to sweep for the block-side prune, holding series fixed.
/// Ninety blocks is a week and a half, which is where a candidate-block scan
/// that is linear in blocks starts to show.
const BLOCK_COUNTS: [usize; 3] = [12, 90, 360];

fn index_prune(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("index_prune");

    for series in SERIES {
        let index = populated_index(series, BLOCKS);
        group.throughput(Throughput::Elements(
            u64::try_from(series).expect("a series count fits a u64"),
        ));

        // One series out of `series`. Expected flat.
        let single = [LabelMatcher::new("pod", MatchOp::Eq, "pod-7")];
        group.bench_with_input(
            BenchmarkId::new("resolve_one_series", series),
            &series,
            |bencher, _| {
                bencher.iter(|| {
                    let fingerprints = index
                        .resolve(TENANT, &single)
                        .expect("the matcher selects a series");
                    black_box(fingerprints)
                });
            },
        );

        // A sixteenth of `series`. Expected linear.
        let group_matcher = [LabelMatcher::new("job", MatchOp::Eq, "job-3")];
        group.bench_with_input(
            BenchmarkId::new("resolve_one_job", series),
            &series,
            |bencher, _| {
                bencher.iter(|| {
                    let fingerprints = index
                        .resolve(TENANT, &group_matcher)
                        .expect("the matcher selects a job");
                    black_box(fingerprints)
                });
            },
        );

        // A regex matcher, which cannot use the equality posting list and has
        // to walk the label's values. The gap to `resolve_one_job` is the
        // price of a regex, and it is a price that has crept up in every
        // system that has this feature.
        let regex = [LabelMatcher::new("job", MatchOp::Re, "job-(1|2|3)")];
        group.bench_with_input(
            BenchmarkId::new("resolve_regex", series),
            &series,
            |bencher, _| {
                bencher.iter(|| {
                    let fingerprints = index.resolve(TENANT, &regex).expect("the matcher compiles");
                    black_box(fingerprints)
                });
            },
        );

        // The label-metadata path behind `/api/v1/labels`, which reads every
        // series in the tenant rather than a selected subset.
        group.bench_with_input(
            BenchmarkId::new("label_values", series),
            &series,
            |bencher, _| {
                bencher.iter(|| black_box(index.label_values(TENANT, "job")));
            },
        );
    }

    // The block half of pruning, with the series count held still so the
    // parameter under test is the number of blocks a tenant has accumulated.
    for blocks in BLOCK_COUNTS {
        let index = populated_index(10_000, blocks);
        let fingerprints = index
            .resolve(TENANT, &[LabelMatcher::new("pod", MatchOp::Eq, "pod-7")])
            .expect("the matcher selects a series");
        // Half the retention window, so the range prunes rather than selecting
        // everything -- a benchmark over a range covering every block would
        // measure the list and not the pruning.
        let midpoint =
            i64::try_from(blocks).expect("a block count fits an i64") * BLOCK_SPAN_MS / 2;

        group.throughput(Throughput::Elements(
            u64::try_from(blocks).expect("a block count fits a u64"),
        ));
        group.bench_with_input(
            BenchmarkId::new("candidate_blocks", blocks),
            &blocks,
            |bencher, _| {
                bencher.iter(|| {
                    let keys = index.candidate_blocks(TENANT, &fingerprints, 0, midpoint);
                    black_box(keys)
                });
            },
        );

        // The trace index's own pruner, which is a bloom filter and a tag map
        // rather than a posting list, and so has its own curve.
        let traces = populated_trace_index(blocks, 1_000);
        group.bench_with_input(
            BenchmarkId::new("trace_prune_by_tag", blocks),
            &blocks,
            |bencher, _| {
                bencher.iter(|| {
                    let keys = traces.prune_blocks_by_tag(
                        TENANT,
                        "service.name",
                        Some("svc-3"),
                        0,
                        midpoint,
                    );
                    black_box(keys)
                });
            },
        );
    }

    group.finish();
}

/// Loading: how much of a tenant's index a query has to fetch and hold.
///
/// The index is stored as one object per tenant per day, so a query over an
/// hour reads one of them and a query over the whole retention reads all of
/// them. The two benchmarks below are the same load against the same store,
/// differing only in whether the caller names its time range, and the gap
/// between them is what the sharding buys. It should widen with the retention:
/// a whole-index load is linear in the days stored, and a one-day load is flat.
/// A run where both are linear is a layout that has stopped sharding.
fn index_load(criterion: &mut Criterion) {
    /// Series in the loading fixture. Smaller than the pruning sweep's
    /// hundred thousand, because every block here holds all of them and the
    /// whole-index arm has to decode the lot on every iteration.
    const SERIES: usize = 5_000;
    /// Two-hour blocks in a day, which is one shard's worth.
    const BLOCKS_PER_DAY: usize = 12;
    /// Retentions to sweep, in days.
    const DAYS: [usize; 3] = [1, 7, 30];
    const KEY: &str = "index/metrics.json";

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a current-thread runtime has nothing to fail on");
    let mut group = criterion.benchmark_group("index_load");

    for days in DAYS {
        let index = populated_index(SERIES, days * BLOCKS_PER_DAY);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        runtime
            .block_on(index.save(&store, KEY))
            .expect("an in-memory store accepts the shards");
        let day_ms = BLOCK_SPAN_MS * i64::try_from(BLOCKS_PER_DAY).expect("a day fits an i64");

        group.throughput(Throughput::Elements(
            u64::try_from(days).expect("a day count fits a u64"),
        ));
        group.bench_with_input(
            BenchmarkId::new("whole_index", days),
            &days,
            |bencher, _| {
                bencher.to_async(&runtime).iter(|| async {
                    let index = Index::load(&store, KEY).await.expect("the index loads");
                    black_box(index)
                });
            },
        );
        group.bench_with_input(BenchmarkId::new("one_day", days), &days, |bencher, _| {
            bencher.to_async(&runtime).iter(|| async {
                let index = Index::load_for_range(&store, KEY, TENANT, 0, day_ms - 1)
                    .await
                    .expect("the index loads");
                black_box(index)
            });
        });
    }

    group.finish();
}

criterion_group!(benches, index_prune, index_load);
criterion_main!(benches);
