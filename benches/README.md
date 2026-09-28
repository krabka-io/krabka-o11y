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

The `bench` job in [//.github/workflows/ci.yml](../.github/workflows/ci.yml) runs them on a schedule, not on a pull request. This follows the fuzz run, which is scheduled for the same reason: the work is unbounded, and a pull request cannot pay for it.

The pull-request half is the `bench-build` job. It compiles the benchmarks with `cargo clippy` and runs none of them. A benchmark that no longer builds is then a failure on the pull request that broke it, instead of a surprise on the next nightly run.

## The ratchet

[//tools/bench-ratchet.py](../tools/bench-ratchet.py) reads a Criterion run and decides whether it may pass. It applies two gates, the same two [//tools/mutants-ratchet.py](../tools/mutants-ratchet.py) applies to a mutation sweep.

The **structural gate** is the one that matters most. `cargo bench` exits 0 when it measures nothing, so a benchmark that falls out of its `criterion_group!` looks the same as one that ran. [//tools/bench-baseline.txt](../tools/bench-baseline.txt) therefore holds the inventory: every benchmark id the suite must produce. A missing id fails. An id the file does not list fails and asks for a line.

The **numeric gate** is the ratchet. A line that carries a number fails when the measured mean is more than 1.5 times it. That factor is not a performance target. It is the smallest ratio a shared CI runner does not produce on its own. Criterion's default noise threshold is 2%, and a 2% gate on a GitHub-hosted runner fires on scheduling noise several times a week. A gate that fires without a cause gets turned off, which is worse than no gate. This one catches the regression that matters — an algorithm that changed order, an index that stopped pruning — and stays quiet about the rest.

Noise is measured, not assumed. When Criterion's own confidence interval for a benchmark is wider than a quarter of its mean, the numeric gate for that benchmark is skipped and the run says so. A measurement that noisy cannot support a verdict either way.

The checked-in baseline was measured on the 16-core BuildBuddy remote runner.
Its raw Criterion artifact, host shape, toolchain, command, duration, and
checksums are recorded in
[`qualification/milestone-19-benchmarks.json`](../qualification/milestone-19-benchmarks.json).
`tools/mutants-record.py --check` fails when `//tools/bench-baseline.txt` has
a SHA-256 or a benchmark count that is different from the record, so a number
cannot change without a recorded run. GitHub-hosted scheduled runs check only
the inventory, because their hardware is different. Refresh the numbers on the
recorded runner, with the `record` input of the
[benchmark-ratchet workflow](../.github/workflows/benchmark-ratchet.yml):

```bash
tools/bench.sh && tools/bench-ratchet.py --record
```

Check in the lines it prints, and record the run in the JSON file. Lower a number in the same change that made the benchmark faster.

### Verdicts

The ratchet puts each benchmark in one class, and the run takes the worst class. The exit code names that class:

| Exit | Class | Meaning |
| ---: | --- | --- |
| 0 | `pass` | Every benchmark is in its budget, or is too noisy to judge and is in its budget. |
| 1 | `regression` | A benchmark with a steady measurement is over its budget. |
| 2 | | The command line is wrong. |
| 3 | `incomplete` | A benchmark is missing, is not listed, or has an estimates file that cannot be read. The run did not measure what it has to measure, so no number is judged. |
| 4 | `noisy` or `variance` | A benchmark is over its budget, but its confidence interval is too wide to be sure. Or `--confirm` did not see a regression again. This is the runner, not the code. |

`--json <path>` writes the verdict with each benchmark's class, mean, spread, budget, and reason. `-` writes it to standard output, and the annotations then go to standard error.

### Confirming a regression

One slow host can put a benchmark over its budget. `--confirm` reruns only the benchmarks that a previous verdict called `regression`, and fails only when a regression occurs again:

```bash
tools/bench-ratchet.py --json verdict.json          # exit 1
tools/bench-ratchet.py --confirm verdict.json --json confirm.json
```

The rerun calls `tools/bench.sh` once for each bench target, with a filter that holds only the regressed ids. It writes to `benches/target/criterion-confirm`, so the first run stays as it was. Two limits keep it small: `--confirm-limit` (default 8) reruns only the worst regressions, and `--confirm-timeout` (default 900 seconds) is the time for all the reruns together. At that limit, the rerun stops `tools/bench.sh` and every process it started. The exit code is 1 when a regression occurs again, 4 when none does, and 3 when a benchmark was not rerun and none of the reruns regressed.

The rerun uses the `tolerance` and `noise_ceiling` that the first verdict records. If you give `--tolerance` or `--noise-ceiling` with a different value, the command stops with exit 2.

The benchmark-ratchet workflow runs this step after exit 1, with a limit of four benchmarks and ten minutes. The step is a second `bb remote` invocation with runner recycling off, so it does not run in the container that measured the first run. The first step prints the verdict to its log, and the second step reads it from there. The workflow reports exit 4 as runner variance and passes.

## Adding a benchmark

1. Add a `[[bench]]` entry to `Cargo.toml` with `harness = false`.
2. Name the Criterion group after the target. `//tools/bench-ratchet.py --bench <name>` selects a target's ids by that first path segment.
3. Put the fixture in `src/`, not in the benchmark file, and build it from a fixed seed.
4. Run `tools/bench.sh --quick <name>`, then add the ids it reports to `//tools/bench-baseline.txt` as `unseeded`.

## License

Apache-2.0. See [LICENSE](../LICENSE).
