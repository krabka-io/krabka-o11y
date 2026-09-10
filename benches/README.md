# krabka-o11y-benches

Criterion benchmarks for the hot paths of the Krabka observability stack.

Part of [krabka-o11y](https://github.com/krabka-io/krabka-o11y), the Krabka observability stack.

## Overview

This directory measures five operations: block write, block read, index pruning, PromQL range evaluation, TraceQL span filtering, and pprof tree merge. Each one is a path that every query or every ingest goes through, so a change in its cost is a change in the cost of the whole system.

The benchmarks exist because nothing else here measures speed. The differential suites and the golden corpora check that an answer is correct. They say nothing about what the answer costs, and they run at a volume too small to show any cost that matters: a few hundred series and a few thousand samples. Krabka is a columnar block store over object storage. That design gives its benefit at high cardinality and high volume, and its failure modes — an index that stops pruning, a compaction that reads more than it writes, a query that holds a whole result in memory — do not appear below that scale.

## Layout

```
benches/
  Cargo.toml          this workspace, and the `[[bench]]` list
  Cargo.lock          its own lockfile
  src/                fixture builders, shared by the benchmarks
  benches/            one file per `[[bench]]` target
```

`src/` holds the generators. Every fixture is built from a fixed seed, so two runs read the same bytes and a difference between them is a difference in the code.

## Why this is a separate workspace

Criterion is a dev-dependency. Cargo gives a crate's dev-dependencies to its tests as well as to its benchmarks, and Bazel reads the same table through `all_crate_deps(normal_dev = True)` in [//bazel/defs.bzl](../bazel/defs.bzl). A `criterion` entry in `crates/blockstore/Cargo.toml` would therefore link Criterion into every test binary that crate builds, under both build systems. `cargo test --workspace` and `bazel test //...` would both get slower and would get nothing for it.

Kept here, the benchmarks are invisible to both:

- The root workspace names this directory in `exclude`, and its `members` glob is `crates/*`, which does not match it.
- `crate.from_cargo` in [//MODULE.bazel](../MODULE.bazel) resolves the root `Cargo.lock`, not the lockfile beside this file. No BUILD file mentions a benchmark, and `//bazel/defs.bzl` has no macro for a `[[bench]]` target.
- `cargo metadata` on the root workspace reports the same ten members it did before.

This is the arrangement [//fuzz](../fuzz) already has, and for the same reason.

The cost is the `[patch.crates-io]` table, which Cargo reads only from the workspace root it builds. The sibling revisions are therefore repeated here. [//tools/check-fuzz-patch.sh](../tools/check-fuzz-patch.sh) compares this copy and the one in `//fuzz` against the root table, and the `tools` job runs it on every pull request.

## Running

```bash
tools/bench.sh                     # every benchmark, the CI budget
tools/bench.sh --quick             # every benchmark, a few seconds each
tools/bench.sh --quick index_prune # one benchmark
```

`--quick` is the mode for a local run. A full run is minutes of wall clock per target.

The results go to `benches/target/criterion`. Criterion writes an HTML report at `benches/target/criterion/report/index.html`, which the `bench` job uploads as an artifact.

## Where they run

The `bench` job in [//.github/workflows/ci.yml](../.github/workflows/ci.yml) runs them on a schedule, not on a pull request. This follows the mutation sweep and the fuzz run, which are scheduled for the same reason: the work is unbounded, and a pull request cannot pay for it.

The pull-request half is the `bench-build` job. It compiles the benchmarks with `cargo clippy` and runs none of them. A benchmark that no longer builds is then a failure on the pull request that broke it, instead of a surprise on the next nightly run.

## The ratchet

[//tools/bench-ratchet.py](../tools/bench-ratchet.py) reads a Criterion run and decides whether it may pass. It applies two gates, the same two [//tools/mutants-ratchet.py](../tools/mutants-ratchet.py) applies to a mutation sweep.

The **structural gate** is the one that matters most. `cargo bench` exits 0 when it measures nothing, so a benchmark that falls out of its `criterion_group!` looks the same as one that ran. [//tools/bench-baseline.txt](../tools/bench-baseline.txt) therefore holds the inventory: every benchmark id the suite must produce. A missing id fails. An id the file does not list fails and asks for a line.

The **numeric gate** is the ratchet. A line that carries a number fails when the measured mean is more than 1.5 times it. That factor is not a performance target. It is the smallest ratio a shared CI runner does not produce on its own. Criterion's default noise threshold is 2%, and a 2% gate on a GitHub-hosted runner fires on scheduling noise several times a week. A gate that fires without a cause gets turned off, which is worse than no gate. This one catches the regression that matters — an algorithm that changed order, an index that stopped pruning — and stays quiet about the rest.

Noise is measured, not assumed. When Criterion's own confidence interval for a benchmark is wider than a quarter of its mean, the numeric gate for that benchmark is skipped and the run says so. A measurement that noisy cannot support a verdict either way.

**Every id in the baseline reads `unseeded` today, so the numeric gate is dormant and the structural gate is live.** A baseline is a wall-clock number. It is therefore a statement about one machine under one load, and it is valid only for that machine. To seed it, CI needs a runner that is quiet, dedicated, and the same one every night. It does not have one yet. Numbers measured anywhere else — a developer's laptop, a shared build box, a GitHub-hosted runner beside eleven other jobs — are not a baseline for this gate, and writing them in would arm it against noise.

Once such a runner exists, take the lines from it:

```bash
tools/bench-ratchet.py --record
```

Check in the lines it prints. Lower a number in the same change that made the benchmark faster.

## Adding a benchmark

1. Add a `[[bench]]` entry to `Cargo.toml` with `harness = false`.
2. Name the Criterion group after the target. `//tools/bench-ratchet.py --bench <name>` selects a target's ids by that first path segment.
3. Put the fixture in `src/`, not in the benchmark file, and build it from a fixed seed.
4. Run `tools/bench.sh --quick <name>`, then add the ids it reports to `//tools/bench-baseline.txt` as `unseeded`.

## License

Apache-2.0. See [LICENSE](../LICENSE).
