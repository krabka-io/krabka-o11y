# Native-histogram windows — issue 383

The histogram cache now indexes each series by timestamp. Window reads and presence checks use binary searches instead of a filter over the complete cached scan. Rate-family kernels borrow their inputs until bucket reconciliation needs writable histograms. Duplicate resolution moves the last payload instead of copying its buckets.

## Method

- Base: `efb9708d`.
- Changed code: `38ac933c`.
- Driver: [`promql_histogram_profile.rs`](../benches/examples/promql_histogram_profile.rs).
- Runtime: rustc 1.99.1, Linux x86_64, four CPU quota, 16 GiB memory.
- Build: unoptimized development profile, line tables, packed debug information. Both revisions use the root `Cargo.lock`.
- Query: `rate(request_duration_seconds[5m])`, with a one-minute step and a fifteen-second scrape interval.
- Sweep: query duration, series count and positive bucket count vary independently around 30 minutes, 10 series and 32 buckets.
- Each case checks the sample count and bucket rates during warmup. Three timed queries follow. One separate query measures allocations.
- Fixture construction and warmup are excluded. Query execution and result destruction are included.
- Allocation counting runs on a current-thread Tokio runtime. Allocations on other threads are excluded. Counting is disabled during timing; the allocator remains linked.
- Both binaries were built before capture. No compiler ran during capture. Other services on the shared host remained active.

The local build copied the unchanged driver into a temporary `crates/promql/examples/histogram_profile_383.rs` in each checkout. This reused the validated root dependency artifacts. `allocation-counter` 0.8.1 was compiled directly from the cached registry source and supplied with `--extern`. The benchmark workspace remains the normal entry point for release builds.

```bash
mkdir -p /tmp/383-profile-deps
rustc --crate-name allocation_counter --crate-type rlib --edition 2021 \
  "$CARGO_HOME"/registry/src/*/allocation-counter-0.8.1/src/lib.rs \
  --out-dir /tmp/383-profile-deps -C debuginfo=0
cargo rustc -p krabka-promql --example histogram_profile_383 --locked -- \
  --extern allocation_counter=/tmp/383-profile-deps/liballocation_counter.rlib
```

Each binary was copied to a separate path before the next checkout was built. The shared target directory was `/workspace/krabka-o11y/target`. The [run record](evidence/promql-histogram-windows-383/run.json) contains source, lockfile and binary hashes. The [base CSV](evidence/promql-histogram-windows-383/base.csv) and [changed CSV](evidence/promql-histogram-windows-383/after.csv) contain all counters, including peak live bytes.

## Results

All seven cases used fewer allocations and fewer allocated bytes. Allocation counts fell by 0.30–5.54%, and allocated bytes fell by 1.78–8.05%. Peak measured live bytes were unchanged in every case.

The case column is query duration in minutes, series count, then bucket count. Each result cell shows base followed by changed code.

| Case | Mean time (ms) | Allocations | Allocated MiB |
| --- | ---: | ---: | ---: |
| 30/10/32 | 1,480.87 → 1,212.54 | 643,973 → 631,513 | 116.10 → 112.11 |
| 5/10/32 | 261.67 → 359.61 | 129,322 → 126,912 | 23.21 → 22.41 |
| 120/10/32 | 5,651.49 → 6,936.77 | 2,496,597 → 2,447,957 | 451.63 → 436.16 |
| 30/1/32 | 139.21 → 137.34 | 68,158 → 66,825 | 12.25 → 11.55 |
| 30/100/32 | 17,057.55 → 16,027.89 | 6,401,284 → 6,277,196 | 1169.04 → 1104.97 |
| 30/10/4 | 180.97 → 189.28 | 224,960 → 212,500 | 33.09 → 30.43 |
| 30/10/256 | 18,747.95 → 21,342.25 | 4,125,166 → 4,112,706 | 821.16 → 806.58 |

Timing results vary in both directions. A base/fix/fix/base repeat of the 30-minute, 10-series, 32-bucket case also shows host variation:

| Run order | Mean time (ms) | Allocations |
| --- | ---: | ---: |
| base-repeat-1 | 1,710.36 | 643,973 |
| after-repeat-1 | 1,262.86 | 631,513 |
| after-repeat-2 | 1,277.36 | 631,513 |
| base-repeat-2 | 1,354.96 | 643,973 |

The same base binary varied from 1.35 to 1.71 seconds. Allocation counts were identical across repeat captures of each binary. These development-profile timings do not establish a production latency improvement. Use a release-profile capture to quantify it.

The [repeat script](evidence/promql-histogram-windows-383/capture_repeat.py) reads the first complete CSV case from each original binary, then stops its remaining cases. Each captured row includes a complete warmup, three timed queries and one allocation-counted query. The four repeat CSV files record the result.

## Verification

- `bazel run //tools/format`: passed.
- `cargo test -p krabka-promql --lib --locked`: 733 passed, including the new index boundary, duplicate ownership and property tests.
- `cargo test -p krabka-promql --test histogram_windows --test conformance --locked`: 19 conformance entries and one range-query regression test passed.
- The regression test compares cached range results and annotations with 216 independent instant evaluations. It covers 12 functions, offsets, mixed sample types, duplicate timestamps, reset hints, start timestamps, and changing exponential and custom buckets.
- The full local `bazel test //...` cold build was interrupted after 13 repository checks passed. The full workspace gate is queued in PR CI.
