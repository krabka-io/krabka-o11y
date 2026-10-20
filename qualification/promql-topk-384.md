# PromQL topk/bottomk selection — issue 384

The shared interpreter/planner kernel now partitions each group and sorts only
its retained samples. The comparison reads label names and original value bytes
directly instead of allocating hexadecimal label keys on ties. Input positions
break otherwise exact ties, preserving the previous stable sort's selection,
including distinct NaN payloads with identical labels.

## Method

The opt-in `benchmark_k_selection` unit test calls the private production kernel
and a test-only copy of the previous full stable sort with its allocating
comparator. Each case checks the complete ordered result before measurement:
labels, timestamp, drop-name flag and float bits. No production API is added for
benchmark access.

The matrix covers both operators at 1,000, 10,000 and 100,000 series with
k=1,10,100. Each series has three labels. Varied values follow a deterministic
permutation; tied values repeat four distinct values. All samples form one
group. Each recorded value is the mean of five calls after the correctness
check. Input construction and cloning are outside timing. Grouping, comparison,
partition/sort, discarded-sample destruction and output assembly are timed.
Returned-sample destruction is outside timing.

Host: x86_64 Intel Xeon Platinum 8573C, Linux 6.18.55, 4 CPU quota,
16 GiB memory. Toolchain: rustc 1.99.1 (58cf8f9c3 2026-10-13).
Baseline algorithm: `efb9708daef226f52d86f0b74402f6806389963b`.
Both algorithms use identical fixtures and dependencies in the same binary.

The local build uses the dev profile with `krabka-promql` and `krabka-metrics`
compiled at optimization level 3; other dependencies remain unoptimized. These
are kernel cost comparisons, not production query latency estimates or CI
performance budgets. The ignored test can also run in the release profile.

```bash
cargo test -p krabka-promql --lib --locked \
  --config 'profile.dev.package.krabka-promql.opt-level=3' \
  --config 'profile.dev.package.krabka-metrics.opt-level=3'

cargo test -p krabka-promql --lib --locked \
  --config 'profile.dev.package.krabka-promql.opt-level=3' \
  --config 'profile.dev.package.krabka-metrics.opt-level=3' \
  benchmark_k_selection -- --ignored --nocapture
```

The regular tests include an independent full-sort oracle, arbitrary byte label
values and label names, grouping with `by` and `without`, empty input, k=0,
k at and beyond group size, stable duplicate ties, signed zeros, infinities,
NaN signs and payloads, and histogram annotation deduplication. The existing
`param_aggregate_planner_path_matches_interpreter` case exercises topk/bottomk
through both execution paths.

## Validation

- PromQL library suite: 734 passed, 0 failed, 1 ignored (the opt-in benchmark).
  This includes the existing parameterized aggregation planner/interpreter parity
  cases and four new regression/property tests.
- `cargo +nightly fmt --all -- --check` and `git diff --check` pass.
- Strict Clippy (`cargo clippy -p krabka-promql --lib --tests --locked`, with
  the same profile overrides and `-- -D warnings`) is blocked by 20 existing
  diagnostics in 16 unchanged files: unused imports, private interfaces, an
  empty line after an outer attribute, and items after a test module. No
  diagnostics point to files changed here; every diagnostic file was checked
  against the base commit and is unchanged.

## Measurements

The [raw CSV](promql-topk-384.csv) contains all 36 cases. The final run took
104.66 seconds with no compiler running. Representative means at 100,000
series and k=10:

| Operator | Values | Full sort (ms) | Selection (ms) | Ratio |
| --- | --- | ---: | ---: | ---: |
| topk | varied | 87.72 | 51.41 | 1.71× |
| bottomk | varied | 99.95 | 47.52 | 2.10× |
| topk | tied | 4664.26 | 77.14 | 60.46× |
| bottomk | tied | 1918.06 | 57.79 | 33.19× |

These are five-call means from one shared cloud host, without confidence
intervals. Tied full-sort timings vary substantially across k even though the
full-sort work is essentially the same; allocator and scheduling effects are
included. Treat the ratios as local observations rather than stable budgets.
The measurements isolate the aggregation kernel and do not establish an
end-to-end query speedup. Every benchmark case passed its exact-output check.
