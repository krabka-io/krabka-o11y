# Grafana upstream source comparison

This source audit identifies performance techniques that Krabka does not yet use.
It does not qualify a performance gain. Research date: 2026-10-05 UTC.
Krabka source inspected: `848eb6a6f23e92f3ec44b3cb7625db0a9a9eebe8`.

## Source identity

The image versions and digests come from [`MODULE.bazel`](../MODULE.bazel).
Each upstream checkout uses the release tag below. `git ls-remote` and
`git rev-parse HEAD` verified the peeled commit, including annotated tags.
These are release source references. The image digest remains the executable
oracle; this audit did not reproduce the upstream image build.

| Product | Release tag | Source commit |
| --- | --- | --- |
| Mimir | `mimir-3.2.1` | [`e49585d43c6e852225e114bd1ddd98da58a4c060`](https://github.com/grafana/mimir/tree/e49585d43c6e852225e114bd1ddd98da58a4c060) |
| Loki | `v3.7.7` | [`7a40404f32b3e6464c9cfc6cc7dd75a40f3931da`](https://github.com/grafana/loki/tree/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da) |
| Tempo | `v3.0.3` | [`1900ed7bb5cad1a3edc285783d7d4ac4278337dc`](https://github.com/grafana/tempo/tree/1900ed7bb5cad1a3edc285783d7d4ac4278337dc) |
| Pyroscope | `v2.3.1` | [`7aeaa0ff91e83538b3ff0d09bfefb168bddc022d`](https://github.com/grafana/pyroscope/tree/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d) |

## Rank against the measured workload

The current metrics comparison has 1,000 series. It measures the entire
application deployment, broker and MinIO. A technique that reduces one small
label table cannot explain the entire RSS gap.

The preserved mapping capture measures about 68 MiB of resident querier code.
At its final snapshot, querier anonymous memory is 30,732 KiB and MinIO
anonymous memory is 234,544 KiB. These are historical diagnostic snapshots,
not the current branch's performance. The earlier allocation capture measures
16.85 MB of peak querier heap. These records establish different costs:
resident code, allocation traffic and object-store memory.
See the [mapping evidence](../qualification/grafana-memory-mapping-investigation-gcp.json)
and [allocation and object-traffic evidence](grafana-performance-profiling.md#cpu-and-object-store-memory).

| Priority | Opportunity | Expected scope and limitation |
| --- | --- | --- |
| 1 | Reduce repeated object operations under the existing publication deadline | Best match to measured full-stack RSS. Deduplicate unchanged metadata work and batch work already due; preserve the two-second publication contract. No gain is established. |
| 2 | Stream cold rows and retain only needed columns | Missing in several explicit materialization paths. Most useful when the query reads cold blocks; profile that path before changing it. |
| 3 | Store hot samples by series in sealed chunks | Can reduce repeated per-sample identity and label pointers. Larger change, with snapshot and retention risks. |
| 4 | Resolve restrictive postings before negative matchers | Small read-path change. Useful for selective multi-matcher queries; the benchmark's simple selector may benefit little. |
| 5 | Pack each distinct label set | Missing despite whole-series interning. Primarily a high-cardinality memory improvement; unlikely to close the current aggregate gap alone. |

These priorities are inferences from source and existing diagnostics. They are
not measured savings. The separate single-code-generation-unit experiment
already addresses resident code. Its Mimir comparison shows lower CPU and RSS
in all three revision pairs. The Loki control also reduces RSS, but increases
query p99 in all three pairs with disjoint ranges. The global setting is not
retained; a metrics-specific build remains a separate candidate.
See its [qualification record](../qualification/grafana-single-cgu-release-experiment-gcp.json).

## Mimir

### Publication and object-store work

Mimir's default TSDB block duration is two hours. The preserved comparison
configuration also uses two hours. Krabka's measured deployment uses a
two-second maximum flush age. One verified control records 1,178–1,188 S3
requests for Krabka and 26–28 for Mimir. Mimir writes zero object bytes during
that minute; Krabka writes about 4.05 MB. Its MinIO role also uses less memory.
The [source default](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/pkg/storage/tsdb/config.go#L301-L306)
and [preserved traffic ledger](grafana-performance-profiling.md#cpu-and-object-store-memory)
explain this important difference in the workload.

Mimir also implements separate configurable caches for bucket lists, metadata
existence, metadata contents and object subranges. This is more specific than a general
query-result cache. See its
[cache configuration](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/pkg/storage/tsdb/caching_config.go#L43-L105)
and [separate metadata cache operations](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/pkg/storage/tsdb/caching_config.go#L138-L155).
The backend defaults to empty, and the implementation skips metadata caching
when no cache client exists. The verified effective configuration from
[run 37377663184](https://github.com/krabka-io/krabka-o11y/actions/runs/37377663184)
also has empty metadata and chunk-cache backends. This is an available
upstream technique, not the cause of that run's low object-request count.
Krabka already caches Parquet footers and bounds reads. See
[`read_block_row_groups_cached`](../crates/blockstore/src/reader/read_block_row_groups_cached.rs).

Do not increase Krabka's publication age to imitate Mimir's measured RSS.
That would change the publication contract. Instead, trace repeated list,
metadata and maintenance operations. Skip only operations whose unchanged
state is proven, or combine work already due within the current deadline.
Any cache needs tenant isolation, invalidation on publication and deletion,
and the existing missing-block recovery behavior. This audit establishes a
source difference, not a safe new cache implementation.


The first concrete candidate is repeated manifest reads in the metrics
compactor. [`run_compactor_once`](../crates/metrics/src/bin/krabka-metrics/run_compactor_once.rs)
loads the current manifest set. The shared
[`read_compaction_manifests`](../crates/metrics/src/compactor/read_compaction_manifests.rs)
reads and decodes every listed object. The querier's
[existing manifest cache](../crates/metrics-service/src/load_compaction_manifests_filtered_with_cache.rs)
already avoids some repeated reads, but caches by key alone. It does not
validate a listed key against the decoded manifest in the same way as the
compactor. Reuse that pattern only after strengthening the shared reader.

Keep each maintenance pass's fresh listing. Bind cache entries to complete
object identity, including version or ETag where available; use conservative
fallback when identity cannot prove the object is unchanged. Validate the
listed key against the manifest before any deletion or compaction. Bound the
cache, discard removed keys, preserve import-marker visibility and propagate
read or decode errors. A destructive caller cannot blindly adopt a querier
cache. This needs a shared reader below the service dependency boundary or a
small compactor-owned cache. It is not a one-line adapter.

### Packed labels and compressed head samples

Mimir's release build uses `netgo,stringlabels`. Its vendored Prometheus label
implementation stores sorted names and values in one packed string. Lookup
returns slices of that string. It does not allocate a tree node and separate
strings for every label. Its hash uses that internal representation.
See the [build tags](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/Makefile#L254-L259)
and [packed representation](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/vendor/github.com/prometheus/prometheus/model/labels/labels_stringlabels.go#L28-L95).

Krabka's [`Labels`](../crates/blockstore/src/labels/labels_type.rs) owns a
`BTreeMap<String, String>`. The hot head already interns equal label sets,
checks full equality on fingerprint collisions, and shares them with `Arc`.
This avoids repeated whole label sets, but not allocations within each
unique set. See [`intern_series_labels`](../crates/promql/src/in_memory/ingest.rs).
A packed representation is genuinely missing. At 1,000 distinct sets, its
potential is much smaller than at millions of sets. No byte-saving estimate
is justified without a current retained-label ledger.

Prometheus organizes head samples by series, encodes sealed chunks, and maps
older chunks from disk. It can retain compressed samples without a label
pointer on every sample. See
[`memSeries`](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/vendor/github.com/prometheus/prometheus/tsdb/head.go#L2814-L2897),
[XOR chunk creation](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/vendor/github.com/prometheus/prometheus/tsdb/head_append.go#L2165-L2190)
and [sealed chunk mapping](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/vendor/github.com/prometheus/prometheus/tsdb/head_append.go#L2244-L2267).
Krabka's [`FloatRow`](../crates/promql/src/in_memory/float_row.rs) repeats the
fingerprint, label `Arc`, timestamp, value and optional creation timestamp.
[`RowChunks`](../crates/promql/src/in_memory/row_chunks.rs) already shares
sealed chunks, but those chunks contain complete rows rather than per-series
compressed samples. This leaves a larger sample-count-dependent opportunity.

Both changes need independent checks for snapshot immutability, retention,
out-of-order samples, equal-time precedence, stale NaN bits, histograms and
creation timestamps. A label change also needs unchanged serde output,
sorted iteration, replacement on duplicate keys, Unicode and the complete
canonical FNV fingerprint corpus. Do not copy Mimir's xxhash or length limit.
Krabka's canonical hash and input behavior are independent contracts.

### Postings and streaming operators

Mimir first intersects restrictive postings, then subtracts negative postings.
It handles missing labels and empty-matching regexes explicitly. Some regexes
have direct postings fast paths. Krabka's
[`Index::resolve`](../crates/blockstore/src/index/index_type.rs) intersects
matcher results in input order. Its
[`resolve_one`](../crates/blockstore/src/index/tenant_index.rs) can build a
nearly tenant-wide set for a negative matcher before intersection.
See Mimir's [postings algorithm](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/vendor/github.com/prometheus/prometheus/tsdb/querier.go#L295-L440).

A small candidate can start with a restrictive positive posting and test
remaining matchers against that candidate set. It must preserve anchored
regex semantics, absent-label behavior, shard matcher validation and errors.
Measure multi-matcher and high-cardinality workloads; do not assume a gain
for `up` alone.

Mimir's default query engine processes each series through reusable iterators.
It obtains result slices from bounded pools with explicit memory accounting.
Its tests can corrupt returned slices to detect use after return. See the
[default engine](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/pkg/querier/querier.go#L118-L119),
[selector](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/pkg/streamingpromql/operators/selectors/instant_vector_selector.go#L82-L145)
and [bounded pools](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/pkg/streamingpromql/types/limiting_pool.go#L17-L100).
Krabka already has a fused latest-float path, cold-block dominance checks,
full sample-limit fallback and a reused canonical cold-label map. See
[`instant_scan`](../crates/promql/src/merged_store/instant_scan.rs).
Do not add an unbounded process-wide pool. Streaming an existing cold scan is
smaller and avoids retained pool capacity.

Mimir also skips histogram buckets for count, sum and average when the plan
allows it. Subqueries, split-range boundaries, quantiles and trim operators
restore bucket decoding. Krabka's histogram row collector decodes complete
histograms. This is a missing specialized technique, but the float-only
comparison does not exercise it. See the
[plan pass](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/pkg/streamingpromql/optimize/plan/skip_histogram_decoding.go#L30-L75)
and [`collect_histogram_rows`](../crates/promql/src/engine/row_cache/collect_histogram_rows.rs).

## Loki

Loki merges already ordered stream iterators with a loser tree. It reads a
bounded response batch from that iterator instead of materializing all rows
first. Time direction and equal-time label/hash ordering are explicit. See
[ordered merge](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/iter/entry_iterator.go#L224-L280)
and [bounded output](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/iter/entry_iterator.go#L681-L710).
Krabka's [cold stream reader](../crates/observability/src/querier/scan/stream_scans/collect_object_store_stream_log_batches.rs)
collects all SQL batches for a block. The folded response already applies its
limit before JSON allocation and can stop between blocks.

The missing technique is bounded consumption within a block after all
applicable filters. Start with `execute_stream()` and incremental append;
prove response equivalence before attempting early stop. Do not push a bare
SQL `LIMIT` into the scan. Krabka's
[scan contract](../crates/observability/src/querier/scan/stream_scans/stream_plan_scan_sql_for_time_range.rs)
explains parser, delete-filter, interval and response-order hazards. Loki's
iterator ordering also differs from Krabka's established folded response
budget, so transplanting the upstream merge unchanged can change answers.

Loki recognizes finite regex alternatives and reads only their postings.
Krabka's shared index runs the regex against every distinct value. A safe
finite-alternative optimization is missing, but needs a real regex parser or
an existing dependency helper; splitting strings at `|` is incorrect.
See Loki's [index lookup](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/ingester/index/index.go#L291-L334)
and Krabka's [`resolve_regex`](../crates/blockstore/src/index/tenant_index.rs).

Krabka already shares hot WAL records and omits source-label copies for normal
queries. `distinct` and tail retain those labels. Those measured changes
address an observed allocation source; do not discard their ownership guards.
See the [Loki experiment record](grafana-performance-profiling.md).

## Tempo

Tempo checks Parquet dictionaries, column chunks and page bounds before it
decodes values. Its iterators can seek to matching row numbers. A second
fetch pass reads additional fields for rows that survive the first pass.
See [dictionary predicates](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/pkg/parquetquery/predicates.go#L67-L101),
[page pruning](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/pkg/parquetquery/iters.go#L649-L724)
and [second pass](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/tempodb/encoding/vparquet4/block_traceql.go#L1600-L1614).
That release uses [vParquet4 by default](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/tempodb/encoding/versioned.go#L97-L104).

Krabka already selects blocks and row groups, promotes attribute columns,
and batches live spans. Its explicit
[Parquet row-group reader](../crates/blockstore/src/reader/read_block_row_groups_cached.rs)
does not install a projection mask or row filter. The
[span store](../crates/traces/src/querier/store/krabka_span_store.rs) collects
cold rows, deduplicates them, recomputes nested sets, then filters matchers.
This leaves predicate and projection work after materialization.

Try column projection or dictionary pruning first for a guarded leaf
selector. Parent, sibling and descendant relations, `childCount`, root
metadata, unscoped attributes and dynamic promoted columns need complete
context. Recomputing nested sets after an unsafe early filter gives a wrong
answer. Read the block's actual schema, retain unknown-column fallback and
corrupt-block errors, and compare complete trace results to the pinned image.
This applies to cold queries; the previous hot-span batch optimization is
already implemented and should not be proposed again.

## Pyroscope

Pyroscope interns profile symbols and aggregates sample values by stack ID
before symbol resolution. Its sample accumulator uses a small hash map and
switches to chunked dense storage for larger sets. It also memoizes positive
and negative call-site matches by stack ID, with a direct function-name
lookup table. See
[symbol rewriting](https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/phlaredb/symdb/dedup_slice.go#L34-L112),
[adaptive accumulator](https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/phlaredb/symdb/sample_appender.go#L9-L100)
and [call-site memoization](https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/phlaredb/symdb/stacktrace_selection.go#L51-L119).

Krabka already interns symbols and groups ordinary tree queries by partition
and stack ID before resolution. See
[`apply_record`](../crates/profiles/src/hot_store/apply_record.rs) and
[`merge_scan_to_tree`](../crates/pprof/src/engine/merge_scan_to_tree.rs).
Its trace-selector branch retains individual samples. The
[tree merger](../crates/pprof/src/engine/merge_sql_to_tree.rs) resolves frames
for each resulting row, and [symbol resolution](../crates/pprof/src/symbol_db/symbol_db_type.rs)
constructs owned function and filename strings. A bounded, query-local cache
can avoid repeated resolution or call-site tests when IDs repeat.

Key such a cache by both partition and stack ID, and bind it to the captured
symbol resolver. Preserve trace/span selection, prefix frames, inline frames,
empty stacks, negative values and exact tree output. Ordinary grouped queries
may already resolve each ID once, so measure the repeated-ID trace path first.
The upstream dense accumulator alone does not justify replacing DataFusion:
the earlier direct-table experiment was rejected. The benchmark's small stack
set also does not justify an adaptive dense-set abstraction.

## Excluded repetitions and next evidence

The branch already has whole-series label interning, the latest-float path,
randomized temporary-map hashing, Loki WAL sharing and source-label omission,
and Tempo live-span batching and borrowed span access. These are implemented
techniques, not new discoveries. Dense metadata, canonical FNV folding,
cold-label sharing, Loki owned shard indexes and Pyroscope direct-table or
borrowed-fingerprint variants did not establish repeatable gains. Their
qualification records remain under `qualification/`.

The repeated cold-fingerprint reuse control gives lower median query p99 in
both runs, but not a repeated aggregate CPU or RSS gain. All ranges overlap.
Its [record](../qualification/grafana-cold-fingerprint-reuse-experiment-gcp.json)
is separate from a claim that the remaining RSS gap is solved.


For the first experiment, change only the metrics manifest-read reuse path.
Use an existing unmodified reader as the baseline. The candidate must retain
fresh listings and decoded key validation. Keep the same compiler setting,
upstream image, publication deadline, harness and runtime limits on both
sides. Capture per-role CPU and RSS, S3 request counts, object bytes, query
p99 and ingest p99 in three alternating revision pairs. Confirm any small
or mixed result before retention. A lower S3 request count alone does not
prove lower MinIO RSS.

The behavior ledger should cover unchanged objects, replaced content at the
same key, absent identity metadata, removed keys, fresh import markers,
malformed bytes and a manifest that names another key. Compare complete
compaction and retention results, including failures. A negative control that
reuses solely by key should fail the replacement case. Run the native metrics
deployment and Mimir/Prometheus differential cases after the shared-reader
checks. Subsequent candidates are incremental cold-row consumption, selective
postings, and guarded Tempo projection; packed labels and compressed hot
samples need separate high-cardinality or long-retention profiles.

Select one narrowly guarded candidate. Keep the existing durability deadline
and comparison harness. Run its independent behavior check, relevant native
Docker cases, and an uninstrumented exact-image revision comparison on the
private GCP runners. Use diagnostic profiles to choose the change; use paired
measurements to decide whether to retain it.
