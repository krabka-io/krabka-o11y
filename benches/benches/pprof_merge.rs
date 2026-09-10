//! Merging two pprof call trees.
//!
//! `crates/pprof` is the other crate with the least test mass per source line,
//! and `Tree::merge` is its hot loop: every profile query merges one tree per
//! block it read, so a merge that is worse than linear in the smaller tree
//! turns a wide time range into a timeout. It is also recursive, which is the
//! shape that degrades quietly with depth.
//!
//! The sweep is over stacks, at a fixed depth, and there is a second sweep
//! over depth at a fixed stack count. Two curves rather than one, because a
//! merge has two parameters and a single number cannot tell which of them a
//! regression moved.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use krabka_o11y_benches::profiles::profile_tree;

/// Stacks per tree, at `DEPTH` frames.
///
/// A hundred thousand stacks at this depth is roughly a million tree nodes,
/// and `iter_batched` holds two trees plus a clone of one of them. The
/// million-stack case belongs in the scale suite, not here: it is ten
/// gigabytes of `Node`, which is a statement about a machine's memory rather
/// than about the merge.
const STACKS: [usize; 3] = [1_000, 10_000, 100_000];

/// Frames per stack while the stack sweep runs. Sixteen is an application
/// stack without a runtime's executor frames under it.
const DEPTH: usize = 16;

/// Depths to sweep, at `STACKS_FOR_DEPTH` stacks. `merge` recurses, so depth
/// is the parameter that would turn a regression into a stack overflow rather
/// than into a slow run.
const DEPTHS: [usize; 3] = [4, 16, 64];
const STACKS_FOR_DEPTH: usize = 10_000;

/// The nodes a flamegraph is truncated to, which is the engine's own default.
const MAX_NODES: i64 = 2048;

fn pprof_merge(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("pprof_merge");
    group.sample_size(10);

    for stacks in STACKS {
        // Two seeds, so the trees overlap heavily without being equal. A merge
        // of a tree with itself takes the "child already present" branch every
        // time and never allocates, which is the fast half of the work and not
        // a case a profile query hits.
        let left = profile_tree(stacks, DEPTH, 0x000A_11CE);
        let right = profile_tree(stacks, DEPTH, 0x000B_0B60);

        group.throughput(Throughput::Elements(
            u64::try_from(stacks).expect("a stack count fits a u64"),
        ));
        group.bench_with_input(BenchmarkId::new("stacks", stacks), &stacks, |bencher, _| {
            // `merge` is in place, so each iteration needs its own copy of the
            // left tree. `iter_batched` clones it in the setup phase, which
            // Criterion excludes from the measurement.
            bencher.iter_batched(
                || left.clone(),
                |mut into| {
                    into.merge(&right);
                    black_box(into)
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    for depth in DEPTHS {
        let left = profile_tree(STACKS_FOR_DEPTH, depth, 0x000A_11CE);
        let right = profile_tree(STACKS_FOR_DEPTH, depth, 0x000B_0B60);

        group.throughput(Throughput::Elements(
            u64::try_from(STACKS_FOR_DEPTH).expect("a stack count fits a u64"),
        ));
        group.bench_with_input(BenchmarkId::new("depth", depth), &depth, |bencher, _| {
            bencher.iter_batched(
                || left.clone(),
                |mut into| {
                    into.merge(&right);
                    black_box(into)
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }

    // What a profile query returns once the merging is done. It consumes the
    // tree, so it is batched the same way, and it is here because a flamegraph
    // that is truncated to `MAX_NODES` should cost the same whatever it was
    // truncated from -- a line that rises with the stack count is a truncation
    // happening after the work rather than instead of it.
    for stacks in STACKS {
        let tree = profile_tree(stacks, DEPTH, 0x000A_11CE);
        group.throughput(Throughput::Elements(
            u64::try_from(stacks).expect("a stack count fits a u64"),
        ));
        group.bench_with_input(
            BenchmarkId::new("to_flamegraph", stacks),
            &stacks,
            |bencher, _| {
                bencher.iter_batched(
                    || tree.clone(),
                    |owned| black_box(owned.to_flamegraph(MAX_NODES)),
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, pprof_merge);
criterion_main!(benches);
