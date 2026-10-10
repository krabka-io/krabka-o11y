# Grafana upstream source comparison

This source audit identifies performance techniques that Krabka does not yet use.
It does not qualify a performance gain. Research date: 2026-10-05 UTC.
Initial Krabka source inspected: `848eb6a6f23e92f3ec44b3cb7625db0a9a9eebe8`.
Follow-up review: 2026-10-06 UTC, rebased source
`7bc01c61eef24917d3ca45f4ce546c295680f27d`.

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

The current metrics comparison has 1,000 series. Application CPU and peak RSS
include the Krabka broker and exclude MinIO on both sides. Object storage
receives the same separate budget and its costs remain available as diagnostics.
Historical resource ratios in this audit included MinIO unless explicitly
re-derived; see the [accounting correction](../qualification/grafana-object-storage-accounting.json).

The preserved mapping capture measures about 68 MiB of resident querier code.
At its final snapshot, querier anonymous memory is 30,732 KiB and MinIO
anonymous memory is 234,544 KiB. These are historical diagnostic snapshots,
not the current branch's performance. The earlier allocation capture measures
16.85 MB of peak querier heap. These records establish different costs:
resident application code, application allocation traffic and separately
budgeted object-store memory.
See the [mapping evidence](../qualification/grafana-memory-mapping-investigation-gcp.json)
and [allocation and object-traffic evidence](grafana-performance-profiling.md#cpu-and-object-store-memory).

| Priority | Opportunity | Expected scope and limitation |
| --- | --- | --- |
| 1 | Avoid repeated canonical label hashing in merged metrics scans | Current CPU profiles identify this work. Reuse a cold canonical key only after full label equality; preserve the fallback for different or absent cold labels. No gain is established. |
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

Mimir also passes an existing hash into its series map to avoid repeated
hash calculations. It checks full label equality on both ordinary and
collision entries. See its
[series map](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/vendor/github.com/prometheus/prometheus/tsdb/head.go#L2250-L2272).
Krabka can apply the same principle where its cold reader already returns
canonical keys. The prepared instant-scan candidate reuses such a key only
when the complete hot and cold labels compare equal. Different labels retain
the existing hash calculation. This trades hashing for a tree lookup and
comparison; only a paired measurement can establish a gain. See the
[prepared experiment](../qualification/grafana-equal-cold-hot-label-key-experiment-gcp.json).

### Postings and streaming operators

Mimir first intersects restrictive postings, then subtracts negative postings.
It handles missing labels and empty-matching regexes explicitly. Some regexes
have direct postings fast paths. Its posting union streams sorted inputs
through a [loser tree](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/vendor/github.com/prometheus/prometheus/tsdb/index/postings.go#L678-L747).
Krabka's sequential fallback in
[`Index::resolve`](../crates/blockstore/src/index/index_type.rs) intersects
owned matcher results in input order. Its
[`resolve_one`](../crates/blockstore/src/index/tenant_index.rs) can build a
nearly tenant-wide set for a negative matcher before intersection.
See Mimir's [postings algorithm](https://github.com/grafana/mimir/blob/e49585d43c6e852225e114bd1ddd98da58a4c060/vendor/github.com/prometheus/prometheus/tsdb/querier.go#L295-L440).

Krabka now starts multi-matcher selectors from the smallest non-empty-value
equality posting. It tests other matchers within that set. Broad regex
selectors keep the distinct-value posting scan. If the exact posting covers
the whole tenant, a single broad regex supplies the result without cloning
that exact posting. Regex unions use one bulk tree construction. They still
materialize an owned set; Mimir's union remains an iterator. Invalid matchers
use the original sequential path, so an empty intersection skips the same
later errors. The logs label index also intersects exact postings from the
smallest posting. Its tenant/name/value dictionaries permit borrowed key
lookups, as Loki's nested label dictionaries do. Label names no longer require scanning
every value posting. Pure equalities and absent negative postings return the
already resolved set directly. Other predicates retain their label checks.
It preserves its separate predicate semantics.

The new `index_matchers` and `log_index_matchers` benchmarks cover 1,000 to
one million series. CPU and allocation profiles select this change. See the
[large matcher investigation](grafana-performance-profiling.md#large-selective-matcher-workloads).
These index measurements do not establish a service-level gain over an
upstream system. Single-matcher and broad selectors remain separate controls.

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

The local 100,000-series application CPU capture adds a concrete target for
that label representation work. With no lost samples, metric-label fingerprinting
accounts for 12.11% of self CPU and blockstore-label fingerprinting for 5.32%.
Label-map and head-summary clones remain visible. The capture includes both
writes and queries, so these percentages do not isolate query CPU or establish
an improvement from changing representation.

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

Keep the existing block-fetch concurrency and block-level failure behavior.
The current reader discards a block if a later batch fails, then emits a
warning. Incremental consumption must discard that block's partial rows too.
Row validation errors still fail the query. See the
[concurrent scan caller](../crates/observability/src/querier/scan/stream_scans/execute_stream_query_from_object_store_with_hot_tail_frontier_and_scan_options.rs).
Keep each block's rows separate until its stream ends successfully; merge
them in the existing block order before the final sort and response limit.
Preserve error precedence too: a late read failure prevents row validation
in the collected path. Drain the stream after a row error; a later read
failure must still discard the block and produce its existing warning.

The current comparison usually fills a 1,000-row limit from hot rows before
cold reads. GCP CPU captures `37330880469` and `37371662781` attribute work to hot
matching, allocation and response statistics. Their saved tables give no
cold-collector attribution. Allocation capture `37333046600` records
2,024,592 string-clone calls through the evaluator, matcher and hot appender.
These captures do not prove that cold reads never occur. They support a
smaller first candidate: reuse evaluated labels within a query when the
pipeline and structured metadata are empty. The
[existing statistics path](../crates/observability/src/http/response/parquet_responses/count_loki_stream_result_hot_tail_lines.rs)
already uses this guard. Loki also
[reuses evaluated label results](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/log/labels.go#L590-L628).
Preserve full label equality, rejected matches, level discovery and grouping.
Keep metadata, pipeline, distinct and tail behavior. The call count shows
allocation traffic; it does not establish an RSS saving.

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

The local 20,000-stream cold-query capture points to planning before further
iterator work: `ScalarValue::eq` consumes 80.65% of sampled self CPU, and
`FilterExec::statistics_helper` another 6.30%. At the pinned DataFusion revision
`532cd0376448c94b6a87d03fe2072d5094764704`, `restricted_column` deduplicates
literal `IN` values with `Vec::contains`, before checking whether the column
holds each value once. That performs quadratic comparisons for a large list,
even when the uniqueness condition cannot hold. Krabka now keeps log fingerprint
lists exact through 4,096 values, then uses their first/last range and retains
the exact Rust membership check. It rejects unrelated fingerprint runs before
allocating structured metadata. The metric scan already bounds large lists.
The million-row log fixture includes broad, quarter-selected and roughly
one-sixty-fourth-selected queries. The initially tried 1,024 cutoff regressed
the last case by decoding more Parquet rows; the larger cutoff keeps that
1,563-value selection exact. The DataFusion dependency and SQL `LIMIT` contract
remain unchanged.

The earlier local 20,000-stream Loki write failures are explained by its WAL
disk throttle, rather than a completed performance comparison. The saved log
reports 90.05% disk usage against the default 90% threshold. Both shutdown and
that throttle return `ErrReadOnly` with the text "Ingester is shutting down".
See [the WAL threshold](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/ingester/wal.go#L55)
and [the push checks](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/ingester/ingester.go#L1002-L1012).
The repeat reserves disk space before deployment and preserves that default.

Loki also constructs reusable regex filters before it evaluates log rows.
[`NewFilter` and `parseRegexpFilter`](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/log/filter.go#L608-L681)
simplify suitable expressions into literal filters; the remaining expressions
are compiled by `newRegexpFilter`. Krabka's selector and line-filter constructors
previously validated a regex and discarded it, then compiled it again for each
row. They now retain that compiled regex. A source-value check preserves edits
to their public pattern fields, and equality compares the source fields rather
than compiled state. This change reuses compilation; it does not add Loki's
literal simplifications. Extracted-field comparisons, label-selection patterns
and dynamic template regexes still have separate evaluation paths.

Loki also reuses loaded index objects. Its
[`TSDBIndex`](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/storage/stores/shipper/indexshipper/tsdb/single_file_index.go#L119-L147)
holds an index reader, and
[`indexSet.ForEach`](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/storage/stores/shipper/indexshipper/downloads/index_set.go#L180-L197)
passes existing cached index objects to callbacks while holding a read lock.
Krabka's request preparation previously copied complete label and block
indexes on cache hits and state clones. It now shares immutable `Arc`
snapshots. Replacing or evicting a cache entry leaves an active request's
snapshot valid; tenant keys, TTLs and compaction-frontier generation handling
are preserved. Bounded shard preparation still rebuilds a label index and
remains a separate cost to investigate.

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
Its trace-selector branch retains individual samples. Previously the
[tree merger](../crates/pprof/src/engine/merge_sql_to_tree.rs) resolved frames
for every resulting row, and [symbol resolution](../crates/pprof/src/symbol_db/symbol_db_type.rs)
constructed owned function and filename strings each time.

The merger now reuses symbol resolution and call-site matching for adjacent
equal `(partition, stack ID)` keys within each Arrow batch. The query SQL
orders those keys, so repeated samples share frames without a persistent
cache or another hash lookup. Individual values still enter the tree in their
original order, including negative and zero values. Call-site matching happens
before prefix frames are appended. Tree insertion also borrows function names
for existing children and clones them only for new nodes.

This follows Pyroscope's reuse by stack ID, while retaining Krabka's existing
sample arithmetic. The state belongs to one batch and its captured symbol
resolver. The complete-query `profile_query` benchmark covers four symbol
partitions with overlapping IDs, repeated stacks, trace selection and call-site
filtering, alongside ordinary grouped-query controls. Regression coverage
includes inline frames, empty stacks, signed values, batch boundaries and
prefix filtering.
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

The first manifest-cache candidate is `d26b82c17940fb8e51514101cadf3c1e815484c0`.
Its separate GCP diagnostic captures each accepted 360,000 rows. Compactor
GETs fell from 1,433 to 1,078, and read bytes fell from 47,971,538 to
16,277,064. Both captures issued 180 compactor listings, 134 puts and 268
delete-stream calls. This confirms avoided reads under the existing listing
and publication cadence. Different VMs and instrumentation prevent a CPU,
RSS or latency qualification from these captures. The
[experiment record](../qualification/grafana-manifest-content-cache-experiment-gcp.json)
records the completed exact-image comparison. The cache is rejected: historical
median
CPU, RSS, query p99 and ingest p99 ratios were 0.976, 1.004, 1.069 and
1.082, with MinIO included in the resource totals. Ingest p99 rises in all three
pairs with disjoint ranges. Avoided
reads did not produce a consistent resource or query benefit.

Re-deriving the same verified raw samples with MinIO excluded gives median
candidate/baseline application CPU and RSS ratios of 0.968 and 1.005.
Latency ratios stay unchanged. The native comparison for that historical
candidate is 0.459× Mimir CPU and 1.406× Mimir RSS: 227.12 versus 161.49 MiB.
These are measurements of the preserved candidate, not the current restored
branch. The cache remains rejected for inconsistent resource gains and the
repeatable ingest regression.

MinIO live-heap captures also limit the object-traffic inference. Bootstrap
allocation stacks account for 82.56% of sampled live heap in the baseline
and 82.74% in the cache candidate. Cumulative stacks overlap; do not add
them. Sampled live heap is not RSS, and these runs have no native Mimir
object-store heap capture. The
[cache record](../qualification/grafana-manifest-content-cache-experiment-gcp.json)
preserves the raw profile hashes. MinIO is outside the application budget.
These profiles neither establish
application RSS savings nor qualify an application performance advantage.
