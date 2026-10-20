# Tenant and time scoped manifest refreshes (#382)

The scoped path kept refresh work fixed in this fixture as unrelated tenants and historical days grew. The queried window held three target blocks throughout.

## Method

Run the [profiling example](../benches/examples/metrics_manifest_refresh_profile.rs) from the repository root:

```sh
CARGO_PROFILE_DEV_DEBUG=line-tables-only \
CARGO_PROFILE_DEV_SPLIT_DEBUGINFO=packed \
CARGO_TARGET_DIR=target \
cargo run --locked --manifest-path benches/Cargo.toml \
  --example metrics_manifest_refresh_profile
```

The run used Rust 1.99.1, `InMemory` object storage, and a current-thread Tokio runtime. Each mode ran five cold iterations. Fixture construction preceded measurement. Allocation counts cover the measured thread through `allocation-counter`.

Each block has one series and a 512-byte label value. Queries select the same three target series at a fixed one-second window. Each unrelated tenant contributes one block in that window. Historical target blocks are one to 1,000 days older. Block payloads are empty placeholders: the measured series path reads labels from manifests. The correctness suites use real Parquet blocks.

The global baseline uses the global range loader, builds the block index, merges the empty hot store, and queries the target tenant. Its store contains blocks and `.index` objects. The scoped store also contains the new catalog summaries and calls `RefreshingMetricBlockStore::series`.

LIST pages use a 1,000-object page model over measured `ObjectStore::list` streams. Empty listings count as one page. These counts describe the model; this run captures no provider HTTP traffic. Both modes include the tenant-deletion HEAD check.

## Results

The [raw CSV](metrics-manifest-refresh-382.csv) records every scale. At 10,000 unrelated tenants and 10,000 historical target blocks:

| Path | Modeled LIST pages | Listed objects | GETs | HEADs | Allocations | Allocated bytes | Mean ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Global | 41 | 40,006 | 20,003 | 1 | 1,147,029 | 199,338,690 | 1,943.231 |
| Tenant/time | 12 | 3 | 6 | 1 | 479 | 64,187 | 0.807 |

Across the three scales, the scoped path used 12 modeled pages, three listed objects, six GETs, one HEAD, 479 allocations, and 64,187 allocated bytes. Its mean latency ranged from 0.736 to 0.807 ms.

These timings describe a dev build with allocation instrumentation on a shared machine. Deployment latency needs provider-backed qualification.

## Tradeoffs and coverage

A small store pays additional catalog requests. With three blocks alone, the global path used one LIST and three GETs; the scoped path used 12 LISTs and six GETs. Publication adds one compact catalog PUT per manifest. Advancing windows recheck cached `.index` objects with HEAD to observe retirement.

The time tree has `2^24` millisecond leaves, about 4.66 hours. History inside a selected leaf and spanning entries can add listed metadata. Filename bounds exclude irrelevant payloads before GET and label decoding. The fixture above grows history outside the selected leaf.

Validation completed:

- Bazel metrics and metrics-service unit suites: 356 and 71 tests passed.
- Cargo compaction lifecycle, TSDB decode, TSDB publication, and service import suites: 58 tests passed. One existing Docker test was ignored.
- Clippy for the affected crates and their workspace dependencies, including test targets, passed with warnings denied.
- Repository formatting passed.

`parallel_manifest_reads` retains bounded concurrent reads, moving-window publication and retirement, immutable captured snapshots, and malformed live-manifest coverage. Additional tests cover tenant isolation and invalidation, signed inclusive time bounds, timeless metadata, partial import listings, catalog cleanup during GET, and normalized unbounded cache coverage. Storage-audit and orphan-sweep tests recognize and retain the appropriate catalog summaries.
