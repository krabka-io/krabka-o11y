# Grafana performance on private GCP runners

Measurements use the existing `gcp-ubuntu-24-04-16core` GitHub runner pool
managed with [google-cloud-github-runner](https://github.com/Cyclenerd/google-cloud-github-runner).
Each signal gets a fresh private `e2-standard-16` VM in `us-central1-b`.
Both backends run sequentially on that VM, alternating order across three
fresh deployment pairs. Builds finish before measurement starts.

The [comparison harness](grafana-performance-comparison.md#provenance-and-reproduction)
preserves the optimized image and its source, manifest and configuration
identities, generated deployment files, raw telemetry, and checksums.
Qualification verifies the archive against GitHub's artifact digest and
every raw file against the archived checksum list. All measured entries must
meet the steady objectives, have zero ingest and query errors, return nonempty
query results, and pass the background CPU gate. Telemetry must cover at least
90% of each measured minute. Profiles also require the immutable seed to stay
exact after publication to cold storage.

These are API-accepted, single-node steady-load comparisons with matched
aggregate CPU and memory budgets, including the broker on Krabka. CPU and RSS
include MinIO on both sides. Acknowledgements have different durability
contracts. Writer and cardinality ramps are outside this qualification.

## Verified measurement checkpoint

All four comparisons below passed three paired 60-second repetitions on
2026-10-05. Metrics uses the fused instant-scan release image
`5b40aec6a591aac531f0c18698264f0304f37cdb`; the pooled services retain their
measured `50f03683` image. Independent archive and filesystem verification
checks all 380 filesystem entries and finds only the metrics query binary
changed: the other six application binaries, base layer and runtime
configuration are identical. This permits reuse of the pooled measurements.
Artifact verification checked every archive digest and all 1,448 evidence-file
checksums. These qualify the recorded images, rather than the current branch
head, which contains further experiments. The earlier
hash-based series-discovery experiment `fd1ded9b` passed correctness checks
but did not establish an end-to-end gain and was reverted. Its actual
measurements are preserved in the
[separate experiment record](../qualification/grafana-series-discovery-experiment-gcp.json).

CPU and peak RSS are Krabka divided by the native backend. p99 shows
Krabka/native medians. Values below 1 favor Krabka. These describe the
steady workload and deployment contract above.

| Upstream | Layout | CPU | Peak RSS | Query p99 |
| --- | --- | ---: | ---: | ---: |
| Mimir | split | 0.60× | 1.65× | 25.79 / 22.68ms |
| Loki | all | 0.91× | 1.07× | 58.37 / 83.36ms |
| Tempo | all | 0.82× | 0.88× | 145.37 / 138.52ms |
| Pyroscope | all | 0.42× | 0.89× | 21.51 / 23.67ms |

Mimir's RSS gap remains material, and its median query p99 is still lower.
Loki's RSS is slightly higher
in this deployment. CPU is lower in all four comparisons. Tempo's latest
median p99 is about 5% higher; its three Krabka values (136.41–152.81ms)
overlap the native values (134.21–140.98ms). The earlier comparison of the
byte-identical Tempo binary measured 88.91/101.32ms, so this workload does not
establish a consistent tail-latency advantage. This is a narrow steady-load
qualification, not a long-running production or durability qualification.

Metrics also passed a before/after comparison on the same AMD VM: fusion
lowers median CPU by 17.0% and p99 from 34.37 to 24.10ms, with both improving
in each of three pairs. The earlier Intel comparison of this same fused image
measured 0.67× native CPU, 1.58× RSS and 35.14/28.85ms p99; host differences
are preserved in the [experiment record](../qualification/grafana-fused-instant-scan-experiment-gcp.json).
The later shared-records Loki image measures 0.87× CPU, 1.00× RSS and
54.44/78.17ms p99 in its [own record](../qualification/grafana-loki-shared-records-experiment-gcp.json).
All seven binaries differ in that image, so it cannot replace rows for the
other three signals by binary equivalence. The isolated metrics cache-hit comparison retained the change: median
query p99 fell 7.3%; whole-deployment CPU was unchanged. Its exact images
and full pair ranges are in the live-label experiment record.
The retained borrowed-selection Tempo image measures 0.69× CPU, 0.95× RSS
and 98.01/108.89ms query p99 in its [own record](../qualification/grafana-tempo-borrowed-selection-experiment-gcp.json).
Its isolated control lowers median RSS 2.9%, with all three pairs lower;
CPU and query p99 gains are not established.
The decimal fingerprint formatting experiment was measured and reverted;
its CPU and query latency gains were not established.

Exact sources, image identities, native image pins, each repetition, gates,
run URLs and artifact hashes are in the
[machine-readable performance record](../qualification/grafana-performance-gcp.json).
CPU and allocation evidence is documented in the
[profiling record](grafana-performance-profiling.md).

## Additional changes selected from the first GCP measurements

The Mimir comparison showed that the metrics querier read cold history for an
instant selector even when every series had a newer sample in its hot head.
Cold reads also grew MinIO's resident memory. The selector can now use the
latest hot float sample when the captured cold index proves that every
selected cold block is dominated. A conservative count of the entire window
must fit the sample limit. When only some cold blocks are dominated, the
selector streams just the remaining blocks, including newer cold data and
series absent from the hot head. Cold-only queries, uncertain coverage,
histograms, ambiguous cold ties and tight limits retain the full scan. Simple selectors and
aggregates use this path, including the separate compensated `sum`/`avg`
selector evaluator; composite expressions and range queries retain their
existing scan snapshot behavior. Equal timestamps preserve the first hot
sample, and stale markers remain selected.

Moving evaluation times still require a fresh manifest listing. Decoded
manifests now remain shared immutable objects instead of being deep-copied
for each refresh. If the effective manifest set is unchanged, the store
reuses its existing cold index. New manifest payloads load with up to four
concurrent reads in listing order. Publication, retirement and query-window
filtering keep their existing freshness behavior; the change does not widen
the cache's time window or hide a missing current manifest.

CPU profiling subsequently identified retention pruning as a growing querier
hotspot. The hot head now tracks a conservative minimum sample timestamp,
including late floats, histograms and exemplars. If no row can expire, pruning
returns without scanning the head or copying its captured store. An actual
prune preserves the inclusive retention boundary, recomputes the minimum,
and publishes only after successful completion. This removes repeated
no-op work; it does not eliminate scans when retention actually expires rows.

The allocation profile identified another repeated copy in label resolution.
The engine now requests immutable shared series labels. In-memory stores and
WAL heads retain each row's existing labels through evaluation; merged stores
preserve cold-first fingerprint precedence and deterministic ordering. Other
stores use a default adapter with their existing owned-series behavior. The
owned series API still returns the same label values. This removes hot-label
string copies without extending query windows or retaining a global result
cache. A follow-up allocation capture found that the production refresh
wrapper still used the default owned adapter. The subsequent wrapper
correction forwards shared discovery through its tenant-aware current store;
the current metrics measurement includes that correction. Its separate
allocation follow-up reduces string-clone allocations from 6.12 million to
4.78 million over startup-inclusive captures. The subsequent CPU capture
still attributes 24–37% of querier self samples to series discovery's
ordered-map lookup on repeated row fingerprints. A hash lookup followed by
sorting distinct series reduced this function's self-sample percentage but
did not demonstrate an end-to-end CPU or latency gain. The ordered lookup
is retained; the experiment remains available for analysis.

The Loki response statistics rebuilt an ordered multiset of hot records,
copying each record's labels, timestamp and line after already evaluating the
query for the response. Statistics now consume a multiset of the returned
entries and borrow their lines. Plain selectors with no per-record metadata
reuse output labels once per distinct source label set. Pipelines and metadata
retain per-record evaluation, including synthesized log levels. Duplicate
records still account for only one response entry each.

The moving-window Loki index cache previously expired entries only on a
lookup of the same key. Queries with continuously changing timestamps never
revisited those keys, retaining complete merged index copies. Admission now
removes expired entries from merged-index, shard-range and shard-index caches
using their existing TTLs. Current entries and captured query snapshots keep
the same contents and freshness rules.

Pyroscope cold queries now overlap up to four block reads while preserving
block order, symbol partition identities, and the captured index snapshot.
This bounds concurrent I/O per query. A missing block still fails the whole
query rather than returning a partial flamegraph. Hot/cold handoff coverage
and private query catalogs remain in place.

The logs, traces and profiles comparisons use the shipped `all` service target
with the existing broker, pooling the application roles' resources into one
process. Metrics retains separate roles because its write and query paths
are shipped in separate binaries with no `all` target. Native and Krabka still have identical
aggregate CPU and memory ceilings. This change removes duplicated process
overhead, but also changes per-process ceilings; it is a different deployment
shape from the historical split-role table. The harness can measure either
shape, or both sequentially on the same VM. The logs `all` example now uses the
tenant object-store index, matching its separate querier configuration.

## Validation

The additional Mimir change passed all 54 scoped PromQL and metrics-service
test and Clippy targets, plus the real Prometheus and Mimir differential
suites. Production-store regression tests compare complete results against
the full-scan control and verify that covered queries make no Parquet reads.
They cover conflicting duplicates, out-of-order arrivals, creation timestamps,
stale markers, offsets, range queries, sample and series limits, newer cold
data, missing hot series, and tenant deletion. A negative control using the
earlier implementation fails the aggregate's zero-Parquet-read assertion.
The compensated-sum case checks `1e16 + 1 - 1e16 == 1` against the full-scan
control, so the shortcut retains the existing aggregate kernel's numerical
behavior.

The subsequent manifest-sharing change passed all 36 scoped test and Clippy
targets and both real Prometheus/Mimir suites. Its nine-block regression
checks independent expected sums, bounds concurrent metadata reads, advances
query time across publication and retirement, verifies a captured snapshot
remains stable, and rejects a malformed live manifest.

The selective cold-block change passed all 37 scoped PromQL and metrics-service
test and Clippy targets and both real Mimir/Prometheus differential suites.
Its ten-block regression reads one newer block while
the full-scan control reads all ten and returns the same complete result.
Offsets, explicit evaluation timestamps, equal-time conflicting cold samples
and missing-block warnings also match the full-scan control.

The profile-guided pruning change passed all 33 scoped PromQL and metrics
service test and Clippy targets and both real Mimir/Prometheus differential
suites. An independent timestamp ledger checks all three sample kinds,
multiple tenants, late arrivals, inclusive boundaries, retention changes,
tenant deletion and saturated timestamps. A held-snapshot test checks both
no-op identity and real-prune snapshot isolation. Restoring the previous
copying path makes that test fail while the other 562 unit cases pass.

The subsequent shared-label change passed the same 33 scoped targets and
both real Mimir/Prometheus differential suites. Its engine regression checks
independent complete labels, cold/hot precedence, time and tenant filtering,
series limits, and held labels across pruning and deletion. Restoring owned
label resolution fails its pointer-sharing checks, with all other 563 unit
cases passing.

The production-wrapper correction passed all 35 scoped targets, including
the real Mimir/Prometheus differential suites. Its regression verifies hot
label identity, complete independent engine output, tenant and time filtering,
cold-first precedence after publication, deletion markers and held labels,
with zero Parquet reads. Removing just the wrapper delegation fails the new
regression while the other 62 service unit cases pass.

The subsequent series-discovery change passed the same 35 targets, including
both real Mimir/Prometheus suites. The direct WAL-head check verifies the
complete ordered labels for mixed float and histogram samples against an
independent expected map, including time and tenant filtering. Its ordinary
and experimental unit suites and test Clippy target pass, and the held-label
regression now verifies the expected retention counts as well.

The additional Loki change passed all 115 scoped LogQL and observability test
and Clippy targets, plus the real Loki differential suite. Its regression
checks cover duplicate multiplicity, distinct lines sharing labels, rejected
source labels, metadata that changes output streams, parser and formatting
pipelines, malformed timestamps, tenant isolation, time bounds and compacted
records.

The Loki cache eviction change passed all 379 observability unit cases, both
Clippy targets and the real Loki differential suite. Its regressions exercise
moving windows through the
production querier state, verify independent expected series and blocks,
and retain a captured earlier snapshot. A second test covers merged indexes,
shard indexes and shard ranges, including list-offset coverage. Disabling
admission eviction makes both tests fail: 33 retained entries instead of two,
and three merged snapshots instead of one after the next moving request.

The additional Pyroscope change passed all 33 scoped pprof and profiles test
and Clippy targets and all eight real Pyroscope/Grafana differential tests.
Its nine-block regression compares the complete cold flamegraph with an
independent hot-store result, including symbols and totals, then removes one
block to verify whole-query failure.

Both harness self-tests passed. Local functional checks verified logs and
traces `all` startup, persisted seed recovery, complete writer drain, reader
catch-up, nonempty queries, zero telemetry errors, and aggregate resource
budgets. These local checks are functional evidence, not performance
qualification.

Tempo's batch packing and Pyroscope's handoff and query-session changes are
described in [the earlier optimization record](grafana-performance-optimization.md)
and [the Pyroscope record](pyroscope-performance-optimization.md).
