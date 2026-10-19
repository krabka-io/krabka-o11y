# OTLP HTTP point decoding — issue 369

Before/after measurements of the production `/otlp/v1/metrics` router at 100,
1,000 and 10,000 points per metric. The [raw CSV](metrics-otlp-http-369.csv)
contains all 36 measurements, including allocation count, total allocated bytes
and peak additional live bytes for each request. Fixture and retained state memory
is outside the measured allocation scope.

## Method

- Date: 2026-10-18.
- Host: x86_64 Intel Xeon Platinum 8573C, Linux 6.18.55, 4 CPU quota, 16 GiB memory.
- Toolchain: rustc 1.98.1 (48a229cea 2026-09-01).
- Before: decoder source from `d34a85f3979a6eb6409bec616a85e2c8b9ce84ab`.
- After: this change. The driver, dependencies and build options are identical.
- Three timed requests after one warmup, followed by a separate allocation-counted request.
- A current-thread Tokio runtime keeps allocation accounting on the handler thread.
- Each case has one metric, no resource/scope attributes, and repeated points with the same labels.
- The counting WAL sink checks the warmup record count and retains no records.
- Delta sum state persists across requests; the other sum and histogram cases are cumulative.

The timer covers authentication, HTTP body handling, protobuf decoding, partial
point translation under the tenant accumulator mutex, validation, WAL conversion,
append acknowledgements, response encoding and consumption. Fixture encoding and
router construction are excluded. Allocation counting is disabled for timing;
the instrumented system allocator remains linked. Socket and broker I/O are excluded.

## Build and run

This local comparison uses the dev profile with the metrics crate, OTLP protobuf
types, driver and allocation counter compiled at optimization level 3. Other
dependencies are unoptimized. These are controlled cost measurements, not
production latency estimates or CI performance budgets.

```bash
CARGO_PROFILE_DEV_DEBUG=line-tables-only \
CARGO_PROFILE_DEV_SPLIT_DEBUGINFO=packed \
cargo build --manifest-path benches/Cargo.toml --example metrics_otlp_http_profile --locked \
  --config 'profile.dev.package.krabka-metrics.opt-level=3' \
  --config 'profile.dev.package.opentelemetry-proto.opt-level=3' \
  --config 'profile.dev.package.krabka-o11y-benches.opt-level=3' \
  --config 'profile.dev.package.allocation-counter.opt-level=3'
benches/target/debug/examples/metrics_otlp_http_profile 3
```

The recorded run set `CARGO_TARGET_DIR=/workspace/krabka-o11y/target` to reuse
artifacts. To reproduce the before build, use this same driver and lockfile but
restore `crates/metrics/src/otlp/decode_otlp_inner.rs` and
`crates/metrics/src/otlp/metric_series.rs` from the base commit. Restore the
fixed files before building the after version. Both executables were built
before timing, with no compiler running during measurement.

## Results at 10,000 points

| Kind | Before HTTP mean (µs) | After HTTP mean (µs) | Before allocations | After allocations | Before allocated MiB | After allocated MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| gauge | 1,990,993.96 | 53,554.67 | 410,106 | 380,106 | 8,465.70 | 71.77 |
| sum | 4,206,292.76 | 52,524.49 | 430,106 | 390,106 | 16,858.80 | 72.53 |
| delta_sum | 3,965,879.83 | 55,181.49 | 460,106 | 420,106 | 16,860.87 | 74.60 |
| histogram | 17,168,598.73 | 320,305.79 | 401,870,112 | 1,830,112 | 38,418.02 | 269.45 |
| exponential_histogram | 6,441,245.28 | 46,198.15 | 460,106 | 420,106 | 39,754.95 | 80.49 |
| summary | 2,433,110.82 | 113,006.79 | 860,109 | 830,109 | 8,535.11 | 141.18 |

## Behavior checks

`cargo test -p krabka-metrics --lib otlp --locked` passed 68 tests. The new
comparison matrix checks complete decoded output against the strict decoder for
all five metric kinds, both monotonic and nonmonotonic sums, all supported
temporalities, mixed valid/invalid batches, and accumulation over two requests.
Additional cases cover unknown temporality, missing accumulator state, empty
metrics, all-rejected input, HTTP rejection counts, first-error reporting and
the all-invalid HTTP response without WAL writes.
