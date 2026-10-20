# Delta stream eviction: issue 381

The tenant accumulator selects excess streams with one bounded heap. A single excess stream uses minimum selection with the same tie order. For N streams and K excess streams, selection takes O(N log(K + 1)) time and O(K) space. Removal takes O(K log N) time. An accumulator within its cap needs no scan or allocation. The previous algorithm scanned both maps once per victim.

The comparison order is last-seen time, stream kind, then label key. Sums precede histograms at equal times. Label keys follow their BTreeMap order. Stale cleanup still runs before cap enforcement, at most once per prune interval. An evicted stream starts a new cumulative value when its next delta arrives.

## Reproduce

Run the behavior tests:

```bash
cargo test --locked -p krabka-metrics --lib otlp::
```

Run the manual latency comparison:

```bash
cargo test --locked -p krabka-metrics --lib eviction_latency_under_same_tenant_contention -- --ignored --nocapture
```

Each case preloads 100,000 streams, half sums and half histograms. Seven last-seen times repeat across the label keys. A churn operation adds 1, 100, 1,000 or 10,000 distinct sum streams. Fixture construction and these additions are outside the eviction timer. The test compares the production method with the previous repeated-minimum rule and checks the restored cap. The reference clones only the chosen key. The original code cloned both candidates when both maps held streams.

A second thread signals readiness while the first thread holds the accumulator mutex. That thread adds one delta stream and enforces the cap after it acquires the mutex. Its timer includes the lock wait, its own accumulation and eviction, and thread scheduling. Both threads use the selected algorithm. The comparison measures accumulator operations under the same tenant lock. It excludes HTTP, protobuf decoding, WAL appends and network I/O. These measurements do not establish end-to-end request latency.

## Behavior coverage

The tests compare retained keys against an independent repeated-minimum reference across mixed maps, sum-only maps, histogram-only maps and empty maps. Caps include disabled, one stream, exact size, reductions across both maps, and `u64::MAX`. Separate cases check equal-time ties, shared keys across maps, stale cleanup at the threshold and prune interval, retained cumulative values, changed start times, and restarts after eviction.

## Recorded result

The run used rustc 1.99.1 on Linux x86_64 with an Intel Xeon Platinum 8573C, a four-CPU quota and 16 GiB of memory. Both algorithms ran in the same test executable with the default unoptimized test profile. No compiler ran during the measurement. Each case has one sample, so the values are diagnostic observations, not statistical estimates or production latency budgets. The single-victim values are close enough that these samples do not establish a speedup for that case.

The [raw CSV](metrics-delta-eviction-381.csv) records microseconds. This table shows milliseconds. The bounded-heap row for one new stream uses the minimum-selection path.

| New streams | Reference eviction (ms) | Bounded selection eviction (ms) | Reference concurrent operation (ms) | Bounded selection concurrent operation (ms) |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 14.61 | 15.24 | 31.71 | 30.47 |
| 100 | 641.36 | 15.72 | 649.52 | 29.66 |
| 1,000 | 6,562.33 | 21.45 | 6,568.77 | 35.99 |
| 10,000 | 84,472.58 | 103.45 | 84,480.17 | 121.05 |

The focused OTLP suite passed 57 tests with one manual timing test ignored. The manual timing test then passed separately. `cargo clippy --locked -p krabka-metrics --lib --tests -- -D warnings`, nightly rustfmt on both changed Rust files, and `git diff --check` passed. Bazel was unavailable in the execution environment, so its format and workspace test commands could not run.
