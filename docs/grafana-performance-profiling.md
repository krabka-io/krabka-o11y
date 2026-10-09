# Profiling the Grafana comparison workload

`tools/profile-grafana.py` reuses the comparison deployment, seed, request
encoders, two writers at one request per second each, and four queries per
second. It profiles Krabka's release image on the private
`gcp-ubuntu-24-04-16core` pool. The
[runner provisioner](https://github.com/Cyclenerd/google-cloud-github-runner)
creates a fresh Google Cloud VM for each job.

Run CPU and allocation profiling separately. Instrumentation changes CPU,
latency and RSS; `profile-report.json` explicitly sets `diagnostic_only: true`
and `comparison_qualified: false`. Use an uninstrumented paired comparison
to establish an optimization's performance result.

Current application CPU/RSS comparisons exclude MinIO and include the Krabka
broker. MinIO profiles and costs describe separate object-storage infrastructure.
Historical aggregate resource figures below include MinIO unless explicitly
re-derived in the [accounting correction](../qualification/grafana-object-storage-accounting.json).

Reuse a preserved image to avoid rebuilding or changing the measured binary:

```bash
gh workflow run grafana-comparison.yml --ref codex/grafana-performance \
  -f signal=metrics -f profiling=cpu \
  -f image_artifact_run=37270733261 \
  -f image_artifact_name=comparison-image-metrics-7184a4c9a4a11c95a3605be42d5074560dc5d77e \
  -f deployment_target=split
```

Use `profiling=allocations` for a separate allocation pass. Use `signal=logs`,
`traces`, or `profiles` with `deployment_target=all` to profile the pooled
layout. `signal=all` schedules all four signals on separate VMs. An empty
`image_artifact_run` builds and preserves the selected branch's image first.
Profiling requires one concrete layout; `deployment_target=both` is rejected.

The workflow validates the image artifact's file checksums and Docker image
identity before starting it. The report records the source commit, image
identity, harness hashes, workload, tool versions and host. Evidence includes
request outcomes, container configuration and logs, telemetry, and SHA-256
checksums. The normal comparison path remains the default (`profiling=none`).

## CPU and object-store memory

CPU profiling captures three consecutive 55-second Linux perf windows from every
Krabka role, including the broker, after seeding and warming the deployment.
The measured workload continues for 180 seconds. `perf record` samples the
software `cpu-clock:u` event at 99Hz. A fresh CPU diagnostic build enables
frame pointers across Rust dependencies and uses `--app-call-graph fp` for
application roles. The image artifact records the compiler flags under its
checksums. Preserved images use frame pointers when their checksums verify
the frame-pointer compiler marker. Images without that verified marker use
`dwarf,16384`. The separately pinned broker always uses DWARF.
`perf_call_graph_by_role` records each role's capture mode.
Frame-pointer diagnostic images cannot be used for ordinary comparisons.
For high-cardinality profiling, set `phases=high_cardinality` and
`max_cardinality` in the workflow. The profiler uses that cardinality and
requests the comparison harness's cold query workload; the report records both
phase and cardinality. These captures remain diagnostic.

This works without a virtual hardware performance counter. Reports resolve
symbols through each live process's container root. Flat reports and cumulative
caller graphs accompany the raw perf data. Precompiled Rust standard libraries
and native libraries can still limit caller coverage. Some recorded mappings
could not be unwound completely,
so cumulative reports do not establish complete caller attribution for those
samples. The first optimization uses resolved flat samples plus the separate
pprof and allocation captures. The preserved image supplies those same binaries for
later offline analysis. The runner needs sudo and Linux tools for its kernel.

`--cpu-profiler pprof` selects the existing admin endpoint instead. One repeat
of in-process pprof sampling on the measured image ended with a querier
segmentation fault (exit 139, no OOM kill); the default uses external perf.
That failed diagnostic run is preserved separately from qualification.

The harness also captures MinIO CPU and heap profiles through its local
[admin Profile API](https://github.com/minio/madmin-go/blob/main/profiling-commands.go),
using curl's AWS SigV4 signer and the synthetic benchmark account. The current
`mc support profile` command requires cluster registration even with
`--airgap`, and the old `mc admin profile` commands only print a deprecation
message. The direct admin API saves the profile ZIP locally and contacts no
support service. Heap summaries include both live bytes
(`inuse_space`) and cumulative allocation bytes (`alloc_space`). Process
`smaps_rollup` snapshots distinguish RSS, private dirty pages and proportional
memory from the Go heap itself.

## Rust allocations

The allocation pass uses [heaptrack](https://github.com/KDE/heaptrack) to
intercept allocations from process startup. It uses the same release binary
and its existing allocator. In a split deployment it instruments the querier;
in a pooled deployment it instruments the signal's `all` process. The other
roles retain their normal binaries and configuration.

The preload library and its dependencies are mounted into the container;
the container keeps its own glibc. The pass records seeding, warm-up and
70 seconds of steady traffic, then stops the instrumented role gracefully.
The raw trace's timestamps distinguish startup from steady activity.
Ubuntu's heaptrack 1.5 interpreter lacks `--sysroot`, so the harness resolves
stacks inside an export of the exact container filesystem. The exported
filesystem is temporary; the artifact retains compressed raw and interpreted
traces, plus a text allocation summary.

Inspect live memory, peak memory, allocation count and temporary allocations
separately. A high allocation count can explain CPU without explaining live
RSS; freed buffers and allocator pages can explain RSS without remaining live
objects. Validate any resulting product change with correctness tests and a
new paired, uninstrumented GCP run.

## First Mimir finding

The initial three 55-second querier CPU captures from
[run 37260662006](https://github.com/krabka-io/krabka-o11y/actions/runs/37260662006)
contain 3.19, 5.55 and 8.77 sampled CPU seconds. `InMemoryMetricStore::prune`
accounts for 33.23%, 39.53% and 38.48% of cumulative sampled CPU. The job's
Krabka CPU captures completed, but its MinIO client failed registration, so
that run is incomplete diagnostic evidence. Its uploaded artifact's outer
digest and every raw file checksum were independently verified.

The external perf metrics capture in
[run 37263608464](https://github.com/krabka-io/krabka-o11y/actions/runs/37263608464)
also completed successfully using the preserved `1c31b683` release image.
Its three querier windows contain 382, 609 and 839 samples, with zero lost
samples. `prune` accounts for 21.47%, 28.41% and 30.87% of **self** CPU;
chunk-retention helpers add further samples. This independent capture
confirms that prune cost grows during this workload. The outer artifact
digest and all 234 evidence-file checksums were verified. These percentages
are sampled CPU attribution, not a latency or throughput comparison.

Pruning walked every float and histogram sample to build fingerprint sets,
retained every chunk, and copied the store on every replayed WAL poll, even
when the oldest sample was within the five-minute retention window. A
conservative minimum sample timestamp can prove that such a prune removes
nothing. Late floats, histograms and exemplars lower that bound immediately;
an actual prune recomputes it from surviving rows. Tenant deletion can leave
the bound conservatively low, which causes extra work rather than skipping
required retention. The WAL head keeps its existing `Arc` for a no-op and
preserves publication after successful completion for an actual prune.

The timestamp-ledger test covers mixed sample kinds and tenants, inclusive
boundaries, late records, retention changes, deletion and saturated integer
timestamps. A separate held-snapshot test verifies that an unexpired prune
keeps the same store and that an actual prune cannot change an old snapshot.
Restoring the old no-op copy made that new test fail, with the other 562 unit
tests passing. A subsequent uninstrumented GCP comparison must establish the
performance effect.

The separate allocation pass in
[run 37261831936](https://github.com/krabka-io/krabka-o11y/actions/runs/37261831936)
passed on GCP. The artifact digest and all 130 evidence-file checksums were
verified. Over 105.92 seconds including startup and seed, heaptrack recorded
14,261,468 allocations, 1,584,222 temporary allocations, a 16.85MB peak heap,
and 220.88KB remaining at exit. These are diagnostic totals, including
instrumentation overhead, rather than steady comparison results. String
cloning accounts for 6,131,850 allocations; the leading caller chain includes
`matched_series`, `labels_by_fingerprint_uncached` and `latest_labeled_series`.
The text summary can be demangled with `c++filt -s rust`.

MinIO's final live heap was 191.47MiB: transition state used 64.85MiB,
notification targets 32.81MiB, expiry workers 16.86MiB, and replication workers
about 38MiB. Its start/end process snapshots measured roughly 162/275MiB RSS
and 103/212MiB private dirty pages. This points to object-store worker queues
as a separate memory cost from the Rust querier's live heap; allocation counts
and RSS alone cannot identify that distinction.

The verified [Mimir control for the rejected fingerprint experiment](https://github.com/krabka-io/krabka-o11y/actions/runs/37303786006), source `913fa01b`, further separates this cost. All six native deployments complete 240 queries. Krabka's simultaneous aggregate RSS peaks span 479,020–498,604KiB, and its MinIO role peaks span 248,524–266,964KiB. Mimir's aggregate peaks span 310,556–313,848KiB, with MinIO peaks of 143,388–145,696KiB. Role peaks can occur at different times and must not be summed. Krabka records 1,178–1,188 S3 requests and 4,050,154–4,054,504 bytes written; Mimir records 26–28 requests and zero bytes written. Mimir's captured effective configuration uses a two-hour TSDB block range; Krabka's measured deployment publishes blocks with a two-second maximum flush age. These are API-acknowledgement comparisons with different publication and durability contracts. The memory and object-traffic ledgers guide further work while preserving the measured contracts.

## Completed capture across all four signals

All four jobs in
[run 37263608464](https://github.com/krabka-io/krabka-o11y/actions/runs/37263608464)
passed using the same preserved release image. Independent verification
checked the four GitHub artifact digests and 234 metrics, 162 logs, 162 traces
and 164 profiles evidence-file checksums. These include raw perf records,
resolved reports, MinIO profiles, memory snapshots and workload outcomes.

The third pooled Loki window shows allocator/free routines and copying among
its leading self samples. Tempo likewise shows allocation/copying, with
Arrow schema and column-name lookup also visible. These are investigation
targets, not attribution of the caller responsible for each allocation.
Pyroscope's third window has only 182 CPU samples; small differences in
individual percentages at this load need a larger diagnostic workload before
selecting another product change. Use the uninstrumented comparison for
latency and resource ratios.

The regional CPU quota allows at most four of these 16-core VMs at once.
Keep concurrent workflow dispatches within that capacity. A VM creation can
fail its asynchronous Compute operation with `QUOTA_EXCEEDED` even when the
manager has already acknowledged the queue webhook; such a job can remain
queued after capacity frees. Check the failed operation and retry only jobs
that never started. Do not replace a completed measurement with a profiling
result or silently change runner size to bypass the limit.

The shared-label change carries the hot head's immutable `Arc<Labels>` through
series discovery and engine label resolution, avoiding the string-cloning
path identified above. The default adapter for other stores preserves their
owned labels, and merged stores retain cold-first precedence. The regression
reaches the engine resolution path, checks complete expected labels and
series limits, and retains labels across pruning/deletion. Restoring owned
resolution fails the pointer checks while the other 563 unit cases pass.
All 33 scoped targets and both real Mimir/Prometheus differential suites pass.

The production allocation follow-up in
[run 37269298396](https://github.com/krabka-io/krabka-o11y/actions/runs/37269298396)
passed on the `50f03683` image. Its artifact digest and all 130 evidence-file
checksums were verified. It still recorded 6,122,458 string-clone allocations,
close to the earlier 6,131,850. The stack resolved the missing delegation:
`RefreshingMetricBlockStore::series_shared` used the trait's default owned
adapter, which reached `InMemoryMetricStore::matched_series`. The engine and
underlying stores shared labels, but the production refresh wrapper copied
them. This capture did not demonstrate removal of the cloning path.

The wrapper now delegates shared discovery through its current tenant store,
including the tenant-deletion check. A production-wrapper regression checks
the original hot-label pointer, complete independent PromQL output, time and
tenant filtering, cold-first precedence after manifest publication, and
held labels after deletion, with zero Parquet reads. Removing just this
delegation fails the new test while the other 62 service unit cases pass.
All 35 scoped targets, including both real Mimir/Prometheus differential
suites, pass with the delegation restored.

The corrected-image allocation pass in
[run 37272649805](https://github.com/krabka-io/krabka-o11y/actions/runs/37272649805)
passed on `7184a4c9`. Its artifact digest and 130 evidence-file checksums were
verified. Over 106.34 seconds including startup and seed, allocation calls
fell to 11,998,303 and string-clone allocations to 4,778,871. The owned
`matched_series` caller is absent from its summary. Peak Rust heap is 16.76MB;
the clone reduction does not establish a resident-memory reduction.

The corrected-image CPU pass in
[run 37272647187](https://github.com/krabka-io/krabka-o11y/actions/runs/37272647187)
also passed, with its artifact digest and 234 evidence-file checksums verified.
The three querier windows contain 429, 740 and 1,000 samples, with no lost
samples. Pruning remains below the 0.5% reporting threshold. Shared series
discovery now accounts for 23.54%, 32.16% and 37.10% of self samples. The code
does an ordered-map membership lookup for every retained row, including many
repeated fingerprints. This selected a hash-lookup experiment, preserving
float-first precedence and fingerprint order by sorting distinct series once.

The experiment's CPU follow-up in
[run 37276157021](https://github.com/krabka-io/krabka-o11y/actions/runs/37276157021)
passed on `fd1ded9b`; its artifact digest and all 234 evidence-file checksums
were verified. The three querier windows contain 353, 556 and 763 samples,
with no lost samples. Discovery's self percentages fell to 3.12%, 6.12% and
8.13%, while hashing routines became prominent. Its uninstrumented comparison
in [run 37274237464](https://github.com/krabka-io/krabka-o11y/actions/runs/37274237464)
passed all three pairs and all 239 evidence-file checksums. Whole-deployment
Krabka CPU remained essentially unchanged (0.21777 versus 0.21754 cores),
and median p99 was 36.64ms versus the preceding 33.33ms. The p99 ranges
overlap (33.00–38.51ms versus 32.39–35.55ms), so a regression is not established,
but neither is a gain. The hash lookup was reverted; a changed flat-sample
percentage alone is insufficient evidence of an optimization.

The [experiment record](../qualification/grafana-series-discovery-experiment-gcp.json)
preserves its actual source, measurements, image and checksums.

The next experiment combines the instant selector's separate hot-head walks
for latest samples and labels. It captures one hot snapshot and refreshes the
cold store for the complete label window. Matching repeated samples can reuse
an accepted immutable label pointer; fingerprint equality alone cannot skip
matching. This adds no persistent query cache and keeps existing refresh TTLs.

The production regression compares complete independent results and the full
scan, including cold-first labels, float and histogram series at the excluded
lookback boundary, sample and series limits, tenant deletion and held labels.
A forced fingerprint collision checks differing label pointers. The first
aggregate query lists manifests once rather than twice; removing only the
production forwarding method fails that guard while the other 64 service
unit cases pass. All 35 scoped targets, including the real Mimir and
Prometheus differential suites, pass. Paired GCP comparison and CPU/allocation
follow-ups are required before retaining this experiment as a performance
improvement.

Those follow-ups passed on the exact `5b40aec6` image:
[CPU capture](https://github.com/krabka-io/krabka-o11y/actions/runs/37286209489)
and [allocation capture](https://github.com/krabka-io/krabka-o11y/actions/runs/37286212807).
Their archive digests and all 234 and 130 raw-file checksums were verified.
The CPU windows contain 170, 225 and 288 samples with zero lost samples.
The separate shared-label discovery walk is below the 0.5% flat-report
threshold; the fused latest-float scan now accounts for 16.47–31.25% self CPU,
with hash lookup and label fingerprinting still prominent. The earlier CPU
profile used Intel model 79 and this one used AMD EPYC 7B12, so the decrease
in total samples cannot by itself establish a CPU reduction.

The 105.68-second allocation trace records 11,672,891 allocation calls,
4,762,565 string clones and a 16.44MB peak heap. String clones are essentially
unchanged from the corrected production-wrapper capture's 4,778,871; fusion
must be assessed as removal of duplicate scanning and manifest refresh work.

The uninstrumented [Mimir comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37282607469)
also passed all three pairs, with its archive digest and 239 raw checksums
verified. Krabka/native medians are 0.67× CPU, 1.58× RSS and 35.14/28.85ms
query p99. Ingest p99 remains 50.52/12.41ms. This comparison used an Intel
host and the previous retained comparison used AMD. A same-VM baseline and
candidate comparison is required before attributing these differences to
fusion or replacing the retained performance checkpoint. The
[experiment record](../qualification/grafana-fused-instant-scan-experiment-gcp.json)
preserves the actual source, per-role CPU, all repetitions and this limitation.

The subsequent [same-VM check](https://github.com/krabka-io/krabka-o11y/actions/runs/37289143258)
passed with three fresh baseline/candidate pairs on AMD EPYC 7B12, alternating
the first revision. The baseline is the corrected wrapper image `7184a4c9`;
the candidate is the exact fused image `5b40aec6`. The artifact digest and
all 725 evidence-file checksums were verified, including the separate native
comparison and all six revision deployments. Each revision ingests 120,000
rows and completes 240 queries per pair without errors or empty results.

Median aggregate CPU falls from 0.22139 to 0.18368 cores (17.0%) and p99 from
34.37 to 24.10ms (29.9%). Both improve in every pair; the query p99 ranges
are disjoint, at 32.72–39.18ms and 24.02–26.35ms. Querier CPU falls 31.8%
and MinIO CPU falls 22.0%, consistent with removal of the duplicate scan and
manifest refresh. RSS and ingest p99 vary between pairs, so their lower
medians do not establish a consistent improvement. The fusion is retained
on this evidence and the complete correctness checks, without relying on
profile sample totals from different CPU models.

The fused profile also shows copying of the head's shared weak-label cache.
Label interning now checks a live, equal-label hit before taking a mutable
cache reference. Such hits keep the cache shared with held query snapshots.
Missing labels, hash collisions with different labels and dead weak entries
use the existing mutation and cleanup path. The snapshot regression checks
float, histogram and exemplar hits, unchanged held rows and dead-entry
cleanup. Removing only the fast path fails that regression while the other
565 unit cases pass. All 35 scoped checks, including real Mimir and
Prometheus differential suites, pass. The isolated release-image comparison below establishes the query effect.

The cache-hit image `8541b68f` has verified
[CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37293278069) and
[allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37293280861)
captures. Archive digests and all 234 and 130 raw checksums pass. The CPU
windows contain 173, 253 and 298 samples with zero lost samples; weak-label
cache cloning is below the flat-report threshold in every window. It is also
absent from the allocation summary, which records 11,168,385 allocations,
4,737,242 string clones and a 16.94MB peak heap over 106.05 seconds.
These diagnostics confirm removal of the repeated cache copy. The isolated
comparison below uses the shared-records image as its control; cross-host
profile totals are not attribution of the cache change.

The prune follow-up in
[run 37264751159](https://github.com/krabka-io/krabka-o11y/actions/runs/37264751159)
passed using the `dd67ca1e` image. Its querier windows contain 303, 447 and
614 samples. No `prune` entry exceeds the 0.5% flat-report threshold in any
window, compared with 21.47–30.87% self CPU before the change. The GitHub
artifact digest and all 234 evidence-file checksums were verified. Series
discovery and latest-sample hash lookup are now prominent self samples.
This confirms removal of the no-op prune hotspot; the sample totals are not
a substitute for paired, uninstrumented CPU measurements.

The separate prune-image Mimir comparison passed all three pairs in
[run 37262286601](https://github.com/krabka-io/krabka-o11y/actions/runs/37262286601).
Its median Krabka p99 is 40.81ms versus Mimir's 23.32ms, and RSS is
488.88 versus 302.23MiB. The preceding Krabka measurement was 47.25ms and
510.29MiB. CPU remains roughly 0.26 cores. Thus pruning improves tails and
RSS in this workload, while a material Mimir gap remains.

## Allocation captures for the pooled services

The separate allocation jobs passed for
[Loki](https://github.com/krabka-io/krabka-o11y/actions/runs/37266699448),
[Tempo](https://github.com/krabka-io/krabka-o11y/actions/runs/37266701064) and
[Pyroscope](https://github.com/krabka-io/krabka-o11y/actions/runs/37266914575),
using the same `dd67ca1e` release image. Their GitHub artifact digests and
130, 130 and 132 evidence-file checksums were independently verified.

The metrics row is the earlier `1c31b683` split-querier pass; the other
rows use `dd67ca1e` and the pooled layout. These totals include process
startup, seeding and the 70-second workload; they measure different signal work and do not rank backend efficiency.
Peak heap uses decimal MB and excludes MinIO. Instrumented RSS is excluded
from the comparison table.

| Signal and layout | Trace duration | Allocation calls | String clones | Peak heap |
| --- | ---: | ---: | ---: | ---: |
| Metrics querier, split | 105.92s | 14,261,468 | 6,131,850 | 16.85MB |
| Logs, all | 105.34s | 75,988,258 | 39,998,496 | 50.59MB |
| Traces, all | 105.88s | 75,300,558 | 11,291,035 | 39.52MB |
| Profiles, all | 124.88s | 11,299,844 | 5,551,323 | 5.22MB |

Loki's hot-tail snapshot path (`HotTailBuffer::records_in_range`) is a
prominent string-cloning caller; label-map copying contributes 7.62MB at
the recorded heap peak in that path. Length-prefixed series fingerprint
encoding also produces many temporary allocations. Tempo's attribute
conversion (`block_attr_value` / `push_span_attr`) produces repeated clones;
search response assembly grows span-reference vectors. Pyroscope's
fingerprint-label construction and tree assembly generate temporary string
copies, while Parquet Zstandard decoder contexts are prominent peak live
allocations. These are concrete next investigation targets. A caller's
allocation count alone does not establish its contribution to CPU or RSS.

The machine-readable [profiling record](../qualification/grafana-profiling-gcp.json)
keeps every successful capture's source, run, artifact digest and limitations.


## Shared Loki records experiment

The hot-tail allocation capture identifies repeated owned snapshots in the
HTTP query path. The buffered source now keeps each immutable WAL record in
an `Arc`. Range snapshots copy those pointers, and both filesystem and object
store query kernels borrow the records. The existing owned-record methods
keep their behavior, and other hot-tail sources can use the default adapter.
Wire fields, query predicates, time-bucket selection and arrival order remain
the same.

The regression compares complete independent records and HTTP responses,
including metadata, duplicate lines, tenant filtering and inclusive time
bounds. It holds an old snapshot across offset/time-based compaction and late
arrival, checks fresh results, and proves removed records are released after
the last snapshot drops. Both production query paths use a source whose owned
snapshot method fails. Removing only buffered-source forwarding fails the
pointer-sharing check, with the other 379 library cases passing. Restoring it
passes all 118 scoped checks, including real Loki and Grafana integration and
end-to-end suites. A fresh uninstrumented GCP comparison and allocation capture
must establish the performance effect before this experiment is qualified.

The exact `84351e69` release image passed the three-pair
[Loki comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37286691135),
[CPU capture](https://github.com/krabka-io/krabka-o11y/actions/runs/37289279428)
and [allocation capture](https://github.com/krabka-io/krabka-o11y/actions/runs/37289282882).
Archive digests and all 239, 162 and 130 evidence-file checksums were verified.
The uninstrumented medians are 0.87× native CPU, 1.00× RSS and
54.44/78.17ms query p99. Krabka's application role peaks at 157–159MiB,
compared with 186–193MiB in the preceding comparison. Both comparisons used
AMD EPYC 7B12 VMs; they were separate hosts, so this is not a same-VM source
comparison. Broker and MinIO remain included in the aggregate metrics.

The allocation capture records 73,432,273 calls, 35,403,446 string clones and
a 44.58MB peak Rust heap. The earlier trace recorded 75,988,258 calls,
39,998,496 clones and 50.59MB. Both run for 105.34 seconds and ingest 140,000
rows, but the new instrumented process completes 151 queries versus 121
earlier. Raw allocation totals therefore do not measure matched query work.
The owned hot-tail range snapshot is absent from the new allocation summary.
Decimal length-prefix construction still accounts for 4,513,200 allocation
calls, and pipeline evaluation still clones label maps repeatedly. These are
the next concrete targets.

The rejected decimal-prefix experiment wrote directly into the canonical byte buffer
with the standard I/O formatter, avoiding the temporary decimal `String`.
The encoding and XXH3 input remain identical. An independent byte ledger
covers empty strings, multi-digit lengths, Unicode byte lengths, delimiters
and existing buffer contents. All 120 scoped checks pass, including
block-store unit tests, the observability/LogQL scope and real Loki and
Grafana integration and end-to-end suites. Allocation and uninstrumented
release-image checks below did not establish a gain, and the change was reverted.

The [experiment record](../qualification/grafana-loki-shared-records-experiment-gcp.json)
retains the source actually built, its equivalent rebased commit and all
checks. All seven application binaries differ in this image; earlier metrics,
Tempo and Pyroscope runs cannot qualify those binaries by equivalence.

The decimal-prefix experiment on `ade83dfa` was rejected after
[three same-VM revision pairs](https://github.com/krabka-io/krabka-o11y/actions/runs/37293349017).
Its median CPU ratio to baseline was 1.024 and query p99 ratio 1.023; both
ranges overlap. RSS rose in all three pairs (median ratio 1.010). Ingest p99
fell at the median but not in every pair. All 725 raw checksums and the
workload, source, image, budget and host gates passed. The formatter was
reverted, retaining its independent canonical-byte regression.

Its verified [CPU capture](https://github.com/krabka-io/krabka-o11y/actions/runs/37295933064)
and [allocation capture](https://github.com/krabka-io/krabka-o11y/actions/runs/37296285016)
remain diagnostic evidence. The allocation pass ran 95 queries on Intel
model 79, versus 151 on AMD EPYC 7B12 in the preceding shared-record pass;
its lower startup-inclusive totals do not establish a per-operation gain.
The [experiment record](../qualification/grafana-loki-decimal-prefix-experiment-gcp.json)
preserves both comparisons and exact image provenance.

Fresh Tempo [CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37297342809)
and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37297346540)
captures on `ade83dfa` passed, with 162 and 130 raw checksums independently
verified. The allocation pass recorded 75,809,673 allocations and 11,347,656
string-clone calls over 105.50 seconds including startup and seed, accepting
14,000 spans and completing 138 queries without errors or empty results.
The stacks still reach `Span::clone` from `LiveStore::batches_in_window`.

The next experiment selects and sorts references to stored spans, avoiding
that temporary whole-span copy before output-row encoding. Existing sorting
and nested-set routines accept standard `Borrow<Span>`; complete trace
metadata still uses the full stored trace, and returned Arrow batches own
their payload. All 37 scoped traces/TraceQL checks pass, including the real
Tempo differential suite, Grafana, both corpus wrappers and Clippy. An
independent full-row ledger checks clipped roots, equal-time ordering,
duplicate attributes, reserved resource keys, bytes, events, links, tenant
filtering and output lifetime. The qualified comparison below retains the change for its lower RSS.

The live-label cache hit is now retained after its
[isolated same-VM comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37293323368).
Against the shared-record image, all three pairs lowered query p99: the
median fell from 31.45 to 29.15ms (7.3%), with disjoint ranges. Querier CPU
fell 3.3% at the median, also with disjoint ranges. Whole-deployment CPU was
unchanged (ratio 1.001); RSS was lower in two pairs, so its 2.3% lower median
does not establish a consistent memory gain. All 725 raw checksums and
source, image, workload, host and resource gates passed. The
[experiment record](../qualification/grafana-live-label-hit-experiment-gcp.json)
also preserves the earlier combined control, which spans the shared Loki
change and cannot isolate this optimization.

Fresh Pyroscope [CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37298005269)
and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37298189841)
captures on `ade83dfa` passed, with 164 and 132 raw checksums verified. All
CPU windows have zero lost samples (200, 196 and 207 samples). The
allocation pass records 11,347,172 allocations, 5,641,591 string clones and
a 5.92MB peak heap over 124.98 seconds including startup and seed. It
accepts 1,400 profiles and completes 280 queries without errors or empty
results on AMD EPYC 7B12. `fingerprint_labels` still clones names and
values into a temporary canonical label map before hashing, reached through
the production hot-store caller. Flame-tree construction also copies strings.
These stacks select the next investigation; no new Pyroscope gain is claimed.

The borrowed-selection image `ca99cacd` has verified [CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37299759203) and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37299762062) captures. Archive digests and all 162 and 130 raw checksums pass. The whole-span clone is absent from the allocation summary. It records 73,220,999 allocations and 7,010,365 string clones over 104.90 seconds including startup and seed. Both this pass and the preceding pass accept 14,000 spans on AMD EPYC 7B12, but this pass completes 147 queries versus 138. These totals are not a per-operation comparison; peak Rust heap rises from 35.16 to 41.84MB. The same-VM uninstrumented comparison below establishes the retention decision.

Independent image checks show that only `krabka-traces` differs from `8541b68f`; the other six application binaries, runtime configuration, base layer and all remaining filesystem entries match. All 380 entries were compared. This equivalence does not establish a performance result for the new traces binary.

The Pyroscope fingerprint experiment on `913fa01b` hashes borrowed label pairs in both the sample store and WAL record. A temporary ordered map retains sorted names and the last value for duplicate names; it no longer copies the strings solely to hash them. Owned labels and borrowed pairs share the original FNV-1a encoder with little-endian byte lengths. The input label vectors, sample fields and WAL format are preserved.

Six fixed hash vectors cover empty inputs, sorting, duplicate names, Unicode byte lengths, NULs and delimiter ambiguity. Production sample insertion and WAL record regressions check canonical hashes and complete payloads. Changing duplicate handling to first-value wins fails all three regressions while the other 121 pprof unit cases pass. All 79 scoped checks pass with the correct code, including the real Pyroscope suites with both Grafana versions, Mimir, Prometheus and Clippy.

Its [same-VM revision comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37301689980) uses the verified `ca99cacd` image as baseline. The change is rejected after the four same-VM revision controls described below. Its verified [CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37302942439) and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37302945210) captures pass all 164 and 132 raw checksums. String-clone stacks no longer include label fingerprinting. The allocation capture records 10,721,706 allocations, 4,971,328 string clones and 5.28MB peak Rust heap over 125.66 seconds including startup and seed. Both this pass and the baseline accept 1,400 profiles and complete 280 queries, but they run on Intel Xeon model 79 and AMD EPYC 7B12 respectively. These totals are diagnostics, not a same-host performance comparison. All seven application binaries changed; the other filesystem entries, base layer and runtime configuration match across all 380 entries. Fresh Mimir, Loki and Tempo comparisons also check this shared hash change.

The borrowed-selection change is retained after its [same-VM comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37298567310). All 725 raw checksums and source, image, host, budget and workload gates pass. Each of the six revision deployments accepts 12,000 spans and completes 235 queries without errors or empty results. Candidate RSS is lower in every pair, with a 2.9% lower median (ratio 0.971). Median CPU and query p99 ratios are 0.992 and 0.981, but their ranges overlap and one pair regresses for each; neither has a consistent gain. The native comparison on this image measures 0.69× Tempo CPU, 0.95× RSS and 98.01/108.89ms query p99. The [experiment record](../qualification/grafana-tempo-borrowed-selection-experiment-gcp.json) preserves all values, exact source and image provenance.


The borrowed-fingerprint experiment is reverted after [Pyroscope](https://github.com/krabka-io/krabka-o11y/actions/runs/37301689980), [Mimir](https://github.com/krabka-io/krabka-o11y/actions/runs/37303786006), [Loki](https://github.com/krabka-io/krabka-o11y/actions/runs/37303789728) and [Tempo](https://github.com/krabka-io/krabka-o11y/actions/runs/37303792734) revision controls. All 2,918 raw checksums and image, source, host, resource and correctness gates pass. Every revision deployment accepts the same row count within its signal. Query completions match for Mimir, Loki and Pyroscope; one Tempo pair completes 235 baseline queries versus 236 candidate queries. The [experiment record](../qualification/grafana-pyroscope-borrowed-fingerprint-experiment-gcp.json) preserves every pair, range, native result and provenance entry.

Pyroscope median CPU, RSS and query p99 ratios are 1.017, 0.998 and 1.059. None has a consistent gain. Loki RSS is higher in every pair, with a ratio of 1.008; its median CPU and query p99 ratios are 1.021 and 1.033. Tempo query p99 is lower in all three pairs (median ratio 0.944), but the ranges overlap and one pair has unequal query completions. Mimir's median RSS ratio is 0.977, with one pair worse. These mixed results do not establish an efficiency advantage for the intended Pyroscope change. The original product hashing code is restored; fixed hash vectors and complete sample/WAL regressions remain.

The next Loki experiment consumes the owned shard indexes when merging them, moving their label maps and block descriptors instead of cloning them and dropping the originals. It still recomputes canonical fingerprints through normal insertion and keeps the first descriptor for duplicate block keys. Complete index ledgers cover tenant isolation, duplicate series and blocks, order and Unicode; string-buffer checks cover the production merger and public consuming APIs. Restoring the original copying merger fails its new regression while the other 380 unit cases pass. All 126 scoped checks pass, including the actual Docker suites for Loki and both Grafana versions. After restoring the original Pyroscope hashing code, all 202 combined scoped checks pass on `45b41ba7`, including the real Pyroscope suites with both Grafana versions. Its [Loki comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37307305944) completed on a private GCP runner. The retention decision remains pending the controls below.

The candidate's [CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37309119439) and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37309122926) captures completed successfully. Archive digests and all 162 and 130 raw checksums pass. CPU windows contain 1,224, 1,194 and 1,193 samples with none lost. The copying `tenant_series` path is absent from the allocation summary. The capture records 62,879,271 allocations, 28,960,597 string clones and 40.97MB peak Rust heap over 105.43 seconds including startup and seed. It accepts 140,000 log records and completes 123 queries, versus 151 queries in the preceding shared-record capture, so these totals are diagnostics rather than normalized gains. All seven application binaries changed from `ca99cacd`; runtime configuration, base layer and the remaining filesystem entries match across all 380 entries. Fresh [Mimir](https://github.com/krabka-io/krabka-o11y/actions/runs/37310243340), [Tempo](https://github.com/krabka-io/krabka-o11y/actions/runs/37310246363) and [Pyroscope](https://github.com/krabka-io/krabka-o11y/actions/runs/37310249960) controls use that exact preserved image.

The first Loki comparison completed successfully, with all 725 raw checksums and source, image, host, workload, budget and correctness gates passing. Each of the six revision deployments accepts 120,000 records and completes 239 queries. CPU is lower in all three pairs, with non-overlapping ranges and a 2.2% lower median; the logs process median CPU is 2.8% lower. Aggregate RSS is higher in all three pairs, with a 6.9% higher median. Much of that increase is in MinIO, whose peak RSS is also higher in every pair. Query p99 has a 2.3% lower median, with overlapping ranges and one pair worse; ingest p99 has a 4.1% higher median. The [experiment record](../qualification/grafana-loki-owned-index-experiment-gcp.json) preserves the complete first control, and an [unchanged-workload confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37312043359) repeats the exact images. Both controls will remain in the evidence. Retention is pending.

Its Mimir and Tempo controls also completed successfully, with all 725 raw checksums and source, image, host, resource and correctness gates passing for each. The six Mimir revision deployments each accept 120,000 rows and complete 240 queries; CPU, RSS and query p99 median ratios are 0.995, 1.000 and 0.994, with overlapping ranges and mixed pair results. The six Tempo deployments each accept 12,000 spans and complete 234 queries; CPU, RSS and query p99 median ratios are 0.995, 1.033 and 0.881. Tempo query p99 is worse in one pair and its ranges overlap, so no consistent latency improvement is attributed to the Loki change. The experiment record preserves these controls and their exact-image native comparisons. The Pyroscope control also completed successfully: all 743 raw checksums and source, image, host, resource and correctness gates pass. Each of its six revision deployments accepts 1,200 profiles and completes 240 queries. Median CPU, RSS and query p99 ratios are 0.985, 1.0005 and 1.084, with overlapping ranges and mixed pairs. Its query p99 is higher in two pairs. All four original controls are preserved; the Loki confirmation remains pending.

The next Mimir candidate shares immutable cold-series labels from the index through `MetricBlockStore::series_shared` and the existing merged instant-scan caller. The retained `8541b68f` allocation capture records 273,726 string-clone allocations in that cold-label caller chain. The index stores one `Arc<Labels>` per series; owned APIs still make independent copies, canonical fingerprint ordering and histogram precedence remain, and no cache lifetime changes. Serde's existing `rc` feature preserves the label payload while the binary shard encoder writes the same fields.

A fixed JSON and v2 binary-shard ledger passes on the original index before the change and on the candidate. Regressions cover tenant isolation, stored fingerprint order, first-insertion labels, cloned snapshots, owned-copy mutation and release after the last held query drops. Complete merged-scan samples and label ledgers check histogram-over-float and cold-over-hot precedence. The production refreshing-store regression checks that repeated discovery and the fused scan use the same cold labels while preserving deletion behavior. Removing only the cold store's shared-series forwarding restores the original copying path: both production ownership regressions fail while the other 630 unit cases pass. With forwarding restored, the three affected unit suites pass all 969 cases. All 368 combined validation targets pass, including 17 actual Docker wrappers covering Mimir, Prometheus, Loki, Tempo, Pyroscope, both Grafana versions and TSDB import. The [cold-label experiment record](../qualification/grafana-shared-cold-labels-experiment-gcp.json) preserves the exact validation source, target-list and log hashes and each native wrapper result. Its [GCP comparison and image build](https://github.com/krabka-io/krabka-o11y/actions/runs/37313303710) uses `45b41ba7` as the exact preceding-image baseline to isolate cold-label sharing. That baseline contains the pending Loki candidate; final retained-branch qualification must reflect the separate Loki retention decision. No measured gain is claimed.

The Loki [confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37312043359) completed successfully; all 725 raw checksums and source, image, host, resource, workload and correctness gates pass. Each revision deployment accepts 120,000 records and completes 239 queries. Aggregate RSS is again higher in every pair, with a 1.4% higher median and disjoint ranges. Median CPU, query p99 and ingest p99 ratios are 0.911, 0.931 and 0.850, but CPU is worse in one pair and query p99 is worse in two. Both controls are preserved in the experiment record. These results do not establish an advantage in both CPU and memory; the retention review remains pending.

The Loki owned-index change is rejected after both controls: aggregate RSS rises in every pair in both runs. The original product merger and index APIs are restored; the complete semantic regression and all experiment evidence remain. All 366 combined validation targets pass on the reversion, including 17 actual Docker wrappers for Mimir, Prometheus, Loki, Tempo, Pyroscope, both Grafana versions and TSDB import. Exact source, target-list and log hashes and each native wrapper result are recorded. The cold-label experiment still isolates its change against its exact preceding image; final branch qualification will use the retained product source.

The cold-label image has verified [CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37315302847) and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37315307601) captures, with archive digests and all 234 and 130 raw checksums passing. All fifteen CPU windows have positive samples and none lost. Both captures run on Intel Xeon model 79. The allocation pass accepts 140,000 rows and completes 280 queries without errors or empty results; it records 9,151,226 allocations, 3,392,825 string clones and 16.17MB peak heap over 106.14 seconds including startup and seed. The cold `matching_series` clone stack is absent from the summarized allocation report. This supports the intended copy removal but establishes no measured efficiency gain; the uninstrumented comparisons remain pending. Exact source, image, host, workload and sample counts are preserved in the profiling record.

The same allocation summary shows two 342,000-call string-clone stacks in WAL label-map construction. The production replay function owns the fully decoded record vector, borrows it into the head, then drops it. The next candidate will consume that vector and move its label strings into the existing ingestion logic, preserving decode-before-lock, atomic batch and watermark publication, borrowed APIs, canonical duplicates and snapshots. Its implementation and regressions are prepared outside the live validation worktree; no implementation pass or performance gain is claimed yet.

The cold-label [Mimir control](https://github.com/krabka-io/krabka-o11y/actions/runs/37313303710) completed successfully, with all 725 raw checksums and source, image, host, workload, resource and correctness gates passing. All six revision deployments accept 120,000 rows and complete 240 queries. CPU is lower in every pair, with a median ratio of 0.991 and overlapping ranges. Aggregate RSS has a median ratio of 0.974, with one pair worse. The querier peak RSS is lower in every pair; its CPU is mixed. Query p99 is higher in two pairs, with a median ratio of 0.990. Its exact-image native comparison measures 0.214 cores, 485.16MiB and 28.22ms query p99 for Krabka, versus 0.348 cores, 312.02MiB and 25.69ms for Mimir. These controls remain specific to their measured image; retention awaits the other three signals and final retained-source qualification.

A [new Mimir comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37318871557) builds source `83868003`, including cold-label sharing and the Loki reversion, and compares it against the preserved `ca99cacd` image. Both sources omit the rejected Loki change. This control completed successfully, with 725 verified raw checksums. All six revision deployments accept 120,000 rows and complete 240 queries. Median CPU, RSS, query p99 and ingest p99 ratios are 1.005, 1.019, 0.988 and 1.243. CPU and RSS are mixed; querier RSS is higher in every pair. Query p99 is lower in every pair with overlapping ranges. The native comparison measures 0.198 cores, 475.01MiB and 27.13ms for Krabka versus 0.326 cores, 309.13MiB and 25.31ms for Mimir. Both Mimir controls remain preserved; no consistent whole-stack advantage is qualified.

The cold-label [Loki](https://github.com/krabka-io/krabka-o11y/actions/runs/37316272048) and [Tempo](https://github.com/krabka-io/krabka-o11y/actions/runs/37316277954) controls also completed, with all 725 raw checksums and source, image, host, workload, resource and correctness gates passing in each. Loki accepts 120,000 records and completes 240 queries in every revision deployment. CPU and query p99 are lower in every pair; median ratios are 0.990 and 0.963, with overlapping ranges. RSS has a median ratio of 1.010 and mixed pairs. Tempo accepts 12,000 spans in every deployment; baseline query counts are 235 in each, versus 234, 234 and 235 for the candidate. Its median CPU, RSS and query p99 ratios are 0.999, 0.982 and 1.012. RSS is lower in every pair, but unequal query completions limit causal attribution. Complete paired and native reports remain in the cold-label experiment record; Pyroscope remains pending.

The cold-label [Pyroscope control](https://github.com/krabka-io/krabka-o11y/actions/runs/37316282221) completed successfully, with all 743 raw checksums and source, image, host, workload, resource and correctness gates passing. Every revision deployment accepts 1,200 profiles and completes 240 queries. Median CPU, RSS, query p99 and ingest p99 ratios are 0.987, 1.014, 0.969 and 1.026, with overlapping ranges and mixed pairs. All four original controls are now preserved, totaling 2,918 verified raw checksums. Retention remains pending the new Mimir comparison after the Loki reversion; no gain in every resource or signal is claimed.

The next Mimir candidate consumes the fully decoded WAL batch and moves its label strings into the existing ingestion logic. The production replay still decodes before mutation and publishes the complete batch and watermarks atomically. Borrowed WAL APIs remain available. A complete head ledger covers float, histogram, metadata and exemplar payloads, tenant isolation, canonical duplicate labels, Unicode, snapshots, offsets and weak-label release. A panicking owned iterator leaves no published prefix and subsequent writes still work. The production replay and PromQL query golden passes on the original production source before the candidate and on the candidate. Restoring the copying path only in the owned batch API fails the ownership regression while the other 634 unit cases pass. With the real source restored, all 41 scoped targets pass, including experimental PromQL, trace-to-metrics exemplars and six actual Docker wrappers for Mimir, both Prometheus versions, both Grafana versions and TSDB import. No measured gain is claimed yet.

The owned-WAL exact-image [CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37321844038) and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37323813657) captures completed, with 234 and 130 verified raw checksums. Only the metrics-service binary differs from the preceding image; the other six binaries, runtime configuration and 380 filesystem entries are verified. The allocation summary records 8,415,293 allocations, 2,743,926 string-clone allocations and 16.69MB peak heap over 105.26 seconds including startup and seed; it no longer lists the original borrowed WAL ingestion caller. Baseline and candidate allocation hosts use different processors, so these totals establish no normalized gain.

The first [matched control](https://github.com/krabka-io/krabka-o11y/actions/runs/37323818671) verified all 725 raw checksums. Every revision deployment accepts 120,000 rows and completes 240 queries. Median candidate/baseline CPU, RSS and query p99 ratios are 1.007, 1.034 and 1.032. RSS and query p99 are higher in every pair, while CPU and ingestion are mixed. An [exact-image confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37327589498) repeats the unchanged workload before the retention decision. The [experiment record](../qualification/grafana-owned-metric-wal-experiment-gcp.json) preserves the complete controls and current-image native comparisons.

Fresh current-image native [Loki](https://github.com/krabka-io/krabka-o11y/actions/runs/37325527624), [Tempo](https://github.com/krabka-io/krabka-o11y/actions/runs/37325534706) and [Pyroscope](https://github.com/krabka-io/krabka-o11y/actions/runs/37325539917) comparisons verified 239, 239 and 245 raw checksums. Median Krabka/native CPU, RSS and query p99 ratios are 0.911/0.989/0.740 for Loki, 0.681/0.937/0.875 for Tempo and 0.412/0.927/0.810 for Pyroscope. Every deployment accepts the scheduled record count with zero ingest/query errors or empty queries. Query counts are 240 versus 239 for Loki and 235 versus 232/231/232 for Tempo; Pyroscope completes 240 for both. Ingest p99 ratios are 5.400, 2.942 and 0.054 respectively. The current Mimir native ratios are 0.656 CPU, 1.663 RSS, 1.321 query p99 and 4.049 ingest p99. These are current-image workload observations; they do not establish a gain from the metric WAL change.

The next Loki candidate omits original source labels from ordinary query entries. The verified `37309122926` allocation summary records 1,445,070 string clones through `matching_loki_stream_entry` and the hot-record appender. Its matcher source is byte-identical to the current baseline. `distinct` still keeps the original labels after pipeline rewrites. Tail callers also keep them for live-frame grouping. Both response encodings keep the existing metadata and parsed-label buckets.

A complete response golden passes on the original source and candidate. It covers queries, label rewriting before `distinct`, backfill and live tail frames, both encodings and unchanged input records. The original folded response keeps empty buckets after `distinct`; the candidate preserves that behavior. All 383 candidate unit cases and both Clippy checks pass. Restoring source copies fails one regression while 382 cases pass. Removing `distinct` preservation fails two regressions while 381 cases pass. Removing tail preservation also fails two while 381 pass. The candidate is restored. [Exact-image baseline CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37329585983) and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37329590449) captures run on private GCP runners. All 120 scoped targets pass, including five actual Docker suites for Loki and both Grafana integration and end-to-end versions. No performance gain is claimed.

The owned-WAL [confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37327589498) completed and verified 725 raw checksums. All revision deployments accept 120,000 rows and complete 240 queries. Median candidate/baseline CPU, RSS, query p99 and ingest p99 ratios are 1.004, 0.993, 0.967 and 1.052. RSS is lower in every pair. CPU and query p99 are mixed; ingestion is higher in every pair. The two controls have opposite RSS results and mixed latency. Neither qualifies a consistent whole-stack advantage. The latest native Mimir comparison measures 0.195 cores, 483.51MiB and 26.09ms for Krabka versus 0.310 cores, 292.73MiB and 22.03ms for Mimir. Both controls and native comparisons remain preserved.

The owned metric WAL experiment is rejected. Its two controls do not show a consistent whole-stack CPU, RSS and latency advantage. Both median CPU ratios exceed one. The original head, ingestion and production replay files match the preceding product source byte for byte. The consuming API and pointer-move-only assertions are removed. Complete batch semantics, fixed fingerprints, snapshot and weak-label checks, watermark ranges, panic rollback and the production query golden remain. The batch ledger now exercises the original borrowed API. All 41 scoped targets pass after the reversion. This includes 569 default PromQL cases, 66 metrics-service cases, experimental PromQL, trace-to-metrics exemplars and six actual compatibility Docker wrappers. The Loki source-label experiment is unchanged; its preserved-image comparisons contain the same WAL implementation in both sources.

The Loki source-label candidate allocation capture [37333046600](https://github.com/krabka-io/krabka-o11y/actions/runs/37333046600) completed with 130 verified raw checksums. It accepted 140,000 rows and completed 169 queries, compared with 133 queries in the baseline capture. It recorded 70,001,365 allocations, 31,011,449 string clones and 32.27 MB peak heap. These diagnostic totals include startup and seed. Unequal query counts prevent a normalized gain claim. The uninstrumented revision comparison remains pending.

The ordered instant-scan experiment replaces the latest-series hash map with a standard ordered map. It also removes the intermediate vector and sort. Three GCP querier windows showed repeated fingerprint hashing in the same production scan bytes. A fixed full-sample and label ledger passes against the original code; reversing traversal fails that ledger. The candidate source is restored. Fresh baseline [37335061905](https://github.com/krabka-io/krabka-o11y/actions/runs/37335061905) and candidate [37336179061](https://github.com/krabka-io/krabka-o11y/actions/runs/37336179061) builds will supply the immutable images for the matched comparison. No gain is established yet. See `qualification/grafana-ordered-instant-scan-experiment-gcp.json`.

The ordered instant-scan candidate passed all 41 scoped targets, including six native Docker wrappers with one executed case and zero ignored cases each. The unit suites passed 569 PromQL and 66 metrics-service cases. The reversed-traversal negative failed exactly the new production scan ledger. The restored candidate retains all previous query, limit and deletion checks.

The first Loki source-label revision control [37333051991](https://github.com/krabka-io/krabka-o11y/actions/runs/37333051991) passed 725 raw checksum checks. Each revision accepted 120,000 rows and completed 239 queries in each pair. Candidate-to-baseline medians were 0.9449 for CPU, 0.9600 for peak RSS, 0.9934 for query p99 and 0.9474 for ingest p99. Peak RSS fell in all three pairs with disjoint ranges. Total CPU and query p99 were mixed by pair; logs-role CPU fell in all three. Repeat [37336578599](https://github.com/krabka-io/krabka-o11y/actions/runs/37336578599) is pending. The experiment remains unqualified for retention until that repeat is checked. Both images contain the same later-reverted metric-WAL change, so the current branch still needs final image qualification.

The fresh ordered-scan baseline CPU capture [37335061905](https://github.com/krabka-io/krabka-o11y/actions/runs/37335061905) completed with 234 verified raw checksums. The immutable image contains the retained source after the metric-WAL reversion. Its querier profiles still show fingerprint hashing in each window. Candidate capture and matched image comparison remain pending.

The ordered instant-scan candidate CPU capture [37336179061](https://github.com/krabka-io/krabka-o11y/actions/runs/37336179061) completed with 234 verified raw checksums. Only the metrics-service binary differs from the fresh baseline; the other six binaries and all nonbinary files match. Matched comparison [37338675813](https://github.com/krabka-io/krabka-o11y/actions/runs/37338675813) is running. No performance gain is established.

The Pyroscope direct-table experiment removes SQL parsing from the hot/cold union collector. A complete nine-column batch ledger passes on both original and candidate code. A one-row limit negative fails that ledger; the candidate is restored. Fresh baseline CPU capture [37337410586](https://github.com/krabka-io/krabka-o11y/actions/runs/37337410586) passed 164 raw checksum checks. Its profile binary is byte-identical to the preserved control baseline image from the Mimir candidate. Candidate build/profile [37338630202](https://github.com/krabka-io/krabka-o11y/actions/runs/37338630202) will supply the matched control image. See `qualification/grafana-pyroscope-direct-table-experiment-gcp.json`.

The Loki source-label confirmation [37336578599](https://github.com/krabka-io/krabka-o11y/actions/runs/37336578599) passed 725 raw checksum checks. Candidate-to-baseline medians were 0.9477 for CPU, 0.9523 for peak RSS, 0.8840 for query p99 and 0.9064 for ingest p99. Each revision accepted 120,000 rows and completed 240 queries per pair. CPU, RSS and query p99 fell in all three confirmation pairs with disjoint ranges. Across both controls, peak RSS and logs-role CPU fell in all six pairs. Retain the Loki source-label change. First-control total CPU and query p99 remain mixed and preserved. These controls qualify the isolated change; both images have the same later-reverted metric-WAL experiment, so final source qualification remains separate.

The confirmation native medians for the preserved Loki candidate image were 0.8705 CPU, 0.9444 peak RSS and 50.98 ms query p99 against 91.63 ms for Loki. Ingest p99 remains 4.56 times upstream. These are three-pair single-node API-accepted measurements, including broker and MinIO costs. They do not establish every workload or current-HEAD performance.

The restored Pyroscope direct-table candidate passed all 39 scoped targets. The unit suites passed 123 pprof and 284 profile cases. Both native Docker wrappers executed eight cases each, with zero ignored cases. They cover Grafana rendering, profilecli, legacy and OTLP ingestion, profile types, render output, series and stats. Trace-to-profile associations and the existing merge goldens passed too. The GCP candidate capture and uninstrumented matched comparison still need to complete before a performance retention decision.

The Pyroscope direct-table candidate [37338630202](https://github.com/krabka-io/krabka-o11y/actions/runs/37338630202) completed with 164 verified raw checksums. Only the profiles binary differs from the paired baseline; six other application binaries and all nonbinary files match. Matched comparison [37340439434](https://github.com/krabka-io/krabka-o11y/actions/runs/37340439434) is running. The candidate remains unqualified for performance retention until its uninstrumented results are checked.

The ordered instant-scan [matched comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37338675813) completed with 725 verified raw checksums. Each revision accepted 120,000 rows and completed 240 queries in every pair. Candidate-to-baseline median CPU, RSS, query p99 and ingest p99 ratios were 1.0677, 1.0128, 1.0980 and 0.9790. Total CPU, querier CPU and query p99 rose in every pair with disjoint ranges. RSS and ingestion were mixed. Reject the ordered map and restore the preceding hash-map scan, preserving the full sample and label ledger. The native comparison remains archived as an observation of the rejected image, not retained-source qualification.

After the rebase and ordered-map reversion, all 41 scoped targets passed, including the six native compatibility wrappers with one executed case and zero ignored cases each. The production scan matches the preceding hash-map source byte for byte; the complete sample/label ledger remains.

The next Mimir experiment changes only the temporary latest-series map to the existing [`ahash::RandomState`](https://github.com/tkaitchuck/aHash) implementation. Canonical persisted fingerprints and final result sorting stay unchanged. Fresh profiles of the preceding hash-map scan showed fingerprint hashing in each querier window, with identical scan bytes after the rebase. The candidate passes both unit suites and Clippy checks. Reversing the result sort fails exactly the full production scan ledger; the candidate is restored. All 43 scoped checks are running, including the new deployment coverage from main. Fresh rebased baseline and candidate release/profile runs precede a matched comparison. No performance gain is established yet. See `qualification/grafana-randomized-instant-hash-experiment-gcp.json`.

The first Pyroscope direct-table [matched comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37340439434) verified 743 raw checksums. Each revision accepted 1,200 rows and completed 240 queries per pair. Candidate-to-baseline median CPU, RSS, query p99 and ingest p99 ratios were 1.0134, 1.0052, 0.9639 and 0.9881. CPU and query p99 were mixed. Peak RSS was higher in all three pairs with disjoint ranges. The native candidate ratios were 0.3854 CPU, 0.9259 RSS and 18.60 ms query p99 against 21.66 ms for Pyroscope. No consistent gain is qualified. [Confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37344462684) repeats the unchanged exact images and workload before the retention decision. Both images contain the same rejected Mimir ordered-map change, so the control isolates the profile collector and does not qualify current HEAD.

The fresh rebased Mimir hash-map baseline [37343291419](https://github.com/krabka-io/krabka-o11y/actions/runs/37343291419) completed with 234 verified raw checksums. It accepted 360,000 rows and completed 720 queries with zero errors and empty results. All 15 role/windows had positive CPU samples and zero lost samples. Image source, archive digest, file checksums and seven binary identities are verified. Candidate capture and matched comparison remain pending; these profiles are diagnostics.

The restored randomized-hash candidate passed all 43 scoped targets. PromQL and metrics-service passed 569 and 66 unit cases. The seven actual Docker wrappers executed 18 native cases with zero ignored cases: six existing compatibility wrappers and the 12 deployment cases added by main. Deployment checks cover v1/v2 hot-to-cold publication and restart, native histograms, metadata and exemplars, HA tenant isolation, OTLP, out-of-order samples, frontend cache alignment and remote-read samples/streamed frames. The experimental functions, conformance corpora, query limits, tenant checks, trace-to-metrics exemplars and Clippy checks passed. Performance retention still depends on the GCP exact-image comparison.

The cold-label sharing experiment is rejected after both Mimir controls and all four original signal controls. The first Mimir CPU gain does not repeat; the second control has higher querier RSS in every pair. Other signal results remain mixed and preserved, including unequal Tempo query completions. The six production files match the preceding owned-label source byte for byte. Independent JSON/KBIX wire bytes, label values and order, tenant isolation, first-label precedence, immutable snapshots, live publication/deletion queries and held result lifetimes remain tested. Assertions requiring the rejected cold pointer identity are removed. All-signal reversion validation is pending.

The cold-label reversion passed the blockstore, PromQL and metrics-service unit suites and their Clippy checks. The independent persisted-byte and complete query ledgers pass with the restored owned representation. The 368-target all-signal validation follows before final reversion qualification.

The randomized-hash candidate [37343900919](https://github.com/krabka-io/krabka-o11y/actions/runs/37343900919) completed with 234 verified raw checksums. Only the metrics-service binary differs from the fresh baseline; six other application binaries and every nonbinary image path match. Matched comparison [37347241886](https://github.com/krabka-io/krabka-o11y/actions/runs/37347241886) is live. Its watcher resumed the existing run after a transient GitHub API read failure; no run was duplicated. Both immutable images contain the same subsequently rejected cold-label sharing implementation, so the comparison isolates the hasher and does not qualify current HEAD.

The Pyroscope direct-table [confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37344462684) passed all 743 raw checksum checks. Every revision accepted 1,200 profiles and completed 240 queries per pair. Median CPU, RSS, query p99 and ingest p99 ratios were 0.9764, 0.9969, 0.9885 and 0.9298. CPU and query p99 were lower in all three pairs with overlapping ranges. RSS was higher in two pairs. Across both controls, RSS was higher in five of six pairs and CPU results did not repeat consistently. The candidate is rejected; both complete controls and the nine-column ledger remain. Product reversion follows the running all-signal correctness check. These immutable images do not qualify current HEAD.

The cold-label reversion passed all 368 validation targets. Eighteen actual Docker wrappers executed 53 native cases with zero ignored cases. They cover Mimir, Prometheus, Loki, Tempo, Pyroscope, both Grafana versions, TSDB import and the 12 deployment cases from main. The persisted-byte and complete query ledgers pass on the restored owned labels. One Grafana wrapper failed twice before it passed because its Prometheus plugin was not ready. The existing bounded readiness check now waits for that plugin. Three fresh runs of each Grafana version pass with retries disabled; Clippy also passes. Exact source, target-list and log hashes remain in the experiment record. Current-source performance qualification remains separate.

The randomized-hash [matched comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37347241886) passed 725 raw checksum checks. Each revision accepted 120,000 rows and completed 240 queries per pair. Median CPU, RSS, query p99 and ingest p99 ratios were 1.0065, 1.0074, 0.9519 and 1.1481. All results were mixed. Querier median CPU was 8.5% lower, but its third pair was higher. A confirmation will use the exact images and workload; no performance retention is qualified. Both images contain the same rejected cold-label sharing implementation and do not qualify current HEAD.

The original Pyroscope union collector is restored after both exact-image controls. Its production code matches the preceding source; the complete nine-column batch ledger remains. All 39 scoped reversion checks pass. Pprof and profiles pass 123 and 284 unit cases. Both native Pyroscope wrappers execute eight cases each with zero ignored cases. The randomized-hash [confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37351036927) repeats the preserved images without a new build.

The next fingerprint experiment uses the standard iterator fold over the existing canonical bytes. Fresh querier samples attribute 6.19–10.10% of self CPU to label fingerprints. An offline compiler check shows that fold removes per-byte iterator-state checks and preserves all six independent fixed vectors. These checks establish no measured gain. [Baseline build and profile](https://github.com/krabka-io/krabka-o11y/actions/runs/37351427961) preserve the restored source before a candidate. The experiment record is `qualification/grafana-canonical-fingerprint-fold-experiment-gcp.json`.

The fingerprint fold candidate passes six initial targets, including the blockstore unit suite, profile WAL fingerprint and Clippy checks. All eight fixed vectors pass on both original and candidate code. Two new vectors cover higher length-prefix bytes and long UTF-8 values. A low-byte-only prefix negative passes the six short vectors but fails the first long vector; the candidate is restored. All 374 validation targets are next, including the 18 native Docker wrappers and three cross-signal caller suites. Performance retention still needs the GCP controls for all four signals.

The randomized-hash [confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37351036927) passed 725 raw checksum checks. Each revision accepted 120,000 rows and completed 240 queries per pair. Querier CPU and query p99 were lower in all three pairs with disjoint ranges; median ratios were 0.8777 and 0.8347. Both measures were lower in five of six pairs across the two controls, and both run medians improved. The temporary hasher is retained for that repeated query advantage. Whole-stack CPU, RSS and ingest results remain mixed and preserved; no aggregate resource gain is qualified. The exact-image native comparison measured 25.95 ms query p99 against 26.26 ms for Mimir. Both images contain the same rejected cold-label implementation, so final retained-source qualification remains separate.

The fingerprint-fold [baseline image and profile](https://github.com/krabka-io/krabka-o11y/actions/runs/37351427961) passed archive checks and all 234 raw checksums. The restored source accepted 360,000 rows and completed 719 queries without errors or empty results. Every CPU window has positive samples and none lost. Fingerprints account for 9.55%, 3.26% and 3.82% of querier self samples. This confirms the selection, with no normalized gain claimed. The [candidate build/profile](https://github.com/krabka-io/krabka-o11y/actions/runs/37354008253) and 374-target correctness check are running.

The fingerprint-fold [candidate image and profile](https://github.com/krabka-io/krabka-o11y/actions/runs/37354008253) passed all 234 raw checksum checks. It accepted 360,000 rows and completed 720 queries without errors or empty results. All 15 CPU windows have positive samples and zero lost samples. Both images have equal runtime configuration, base layer and all 380 filesystem paths except the seven verified application executables. The candidate splits fingerprint samples between the function and its iterator fold; their combined shares are 6.79%, 6.01% and 3.46%. These sparse, separate-host diagnostics establish no normalized gain. All-four-signal exact-image comparisons follow the 374-target correctness check.

The fingerprint fold passes all 374 validation targets, including Clippy, conformance, persisted-byte and full query ledgers. Eighteen actual Docker wrappers execute 53 native cases with zero ignored cases. Three cross-signal caller suites also pass with no ignored cases. The Loki Grafana integration wrapper returned HTTP 404 once and passed on its retry. Its failure body was not captured, so the cause remains unverified. Fresh runs with retries disabled follow. Source, target-list and result-log hashes are recorded. The exact-image native and alternating controls remain required before performance retention.

Both Loki Grafana integration wrappers pass three fresh runs each on unchanged source with retries disabled. The original HTTP 404 and successful retry remain preserved; the response body and cause were not captured. The 374-target correctness scope is qualified with this explicit limitation. Performance retention still depends on all four exact-image controls.

The [four-signal fingerprint-fold comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37357671713) uses the two verified preserved images. Each signal runs three native upstream repetitions and three alternating baseline/candidate pairs on private GCP runners, with the existing matched workloads and resource budgets. Metrics uses the split layout; logs, traces and profiles use their all-role layout. The comparison is queued; no performance retention is established yet.

The next block metadata candidate replaces a temporary tree map with vectors indexed by existing live time-order positions. Sparse Mimir querier profiles report this metadata path; they establish no normalized gain. The full metadata ledger passes on original and candidate code. It checks every field through reordered insertion, time restatement, changed fingerprints, tombstones, retention, compaction, snapshots, tenant isolation and JSON restoration. All eight initial unit and Clippy targets pass. Using persistent ordinals in place of time-order positions fails exactly the full ledger; the candidate is restored. All 376 correctness targets are next. Performance controls will use the retained fingerprint source after its current four-signal comparison. The experiment record is `qualification/grafana-dense-block-metadata-experiment-gcp.json`.

The 376-target metadata validation runs on source `e2fa65371cf99eaa7f439dcce86191a1177c9e1c`. The earlier fingerprint comparison continues with its original exact images. No metadata performance gain is qualified.

The restored-source Mimir querier starts with 76 MiB of private clean pages and about 9 MiB of anonymous pages in its diagnostic rollup. The rollup does not identify which file or region owns those pages. The pinned telemetry router creates a CPU sampling guard only for a profile request; the capture uses perf. The profiling harness now preserves each role's full memory mappings at both existing snapshots. A private GCP capture of the exact preserved image will locate the file-backed RSS before a memory optimization is selected. These diagnostics do not establish a performance gain. See `qualification/grafana-memory-mapping-investigation-gcp.json`.

The first two fingerprint-fold signal jobs passed with 725 raw checksum checks each. Mimir median CPU, RSS, query p99 and ingest p99 ratios are 0.9794, 1.0196, 0.9385 and 0.8617. Total CPU is lower in one pair; RSS is higher in all three. Querier CPU and query p99 are lower in two pairs. Loki ratios are 0.9826, 0.9983, 0.9533 and 1.0138; RSS and query p99 are lower in every pair. All ranges overlap. The first Mimir native observation is 28.84 ms query p99 against 27.04 ms, CPU ratio 0.6060 and RSS ratio 1.5982. The Loki native observation is 76.53 ms against 119.88 ms, CPU ratio 0.7872 and RSS ratio 0.9034. Both complete signal artifacts remain preserved. Tempo and Pyroscope still run; confirmation is needed before retention. These images exclude the pending dense metadata change.

The [memory mapping capture](https://github.com/krabka-io/krabka-o11y/actions/runs/37361260446) passed 238 raw checksum checks on the verified restored-source image. All six roles have complete mapping snapshots; all 15 CPU windows have positive samples and zero loss. It accepted 360,000 rows and completed 720 queries without errors or empty results. The querier executable code mapping holds 67.6 MiB at start and 68.1 MiB at end. Anonymous memory grows from 10.2 MiB to 30.0 MiB. Code pages account for most startup RSS; heap-only changes cannot remove that region. The harness preserves the original rollups and full mappings. These snapshots select the next memory investigation and establish no normalized gain.

The branch is rebased onto main `664e6987`; all 115 branch patches remain unchanged. The metadata candidate passes all 376 targets on its original source. Eighteen native wrappers execute 53 cases without ignored cases. The Tempo/Grafana wrapper returned HTTP 404 for `/api/echo` once, then passed on retry. Its response body and cause are unverified. The failed attempt remains preserved; six fresh current and previous Tempo wrapper runs have retries disabled. The new upstream Loki deployment coverage needs separate validation.

The [Mimir fingerprint confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37363580969) repeats both verified images on one private GCP runner. It uses three native repetitions and three alternating revision pairs under the original workload and budgets. Its source images exclude the metadata candidate and the new upstream changes. No additional performance gain is qualified.

All four fingerprint comparison jobs pass. Metrics, logs and traces each pass 725 raw checksum checks; profiles passes 743. Tempo median CPU and query ratios are 0.9370 and 0.8246, lower in every pair. Its query ranges are disjoint. Pyroscope CPU and RSS ratios are 0.9977 and 0.9968. Its query and ingest ratios are 1.0257 and 1.0144; two pairs have higher latency. The Mimir confirmation continues before retention. These measurements qualify the exact preserved images, not the rebased branch or dense metadata candidate.

The exact captured querier executable has SHA-256 `2847a667a6a666f5be183ab2565ccc23ec56e7cbd884cbe2ab26a025769f4dbb`. Its text section is about 121 MiB. Defined text symbols, with shared addresses counted once, include about 20.8 MiB in `sqlparser`. Generic code and inlining limit ownership attribution. Static symbol sizes do not identify resident pages or prove a memory gain. The offline analysis is pinned in the memory investigation record; the release build flags remain unchanged.

The [rebased metadata baseline build/profile](https://github.com/krabka-io/krabka-o11y/actions/runs/37365264758) and [candidate build/profile](https://github.com/krabka-io/krabka-o11y/actions/runs/37365268018) preserve matching release images. Both contain main `664e6987` and the same fingerprint implementation. Their only production difference is the dense metadata function; dependencies and build configuration are equal. The baseline reference is `codex/grafana-metadata-baseline`. These captures are diagnostic only. Correctness qualification and the fingerprint decision remain required before retention.

All seven post-rebase deployment and Clippy checks pass with retries disabled. The Mimir and Loki deployment wrappers each execute 12 cases with zero ignored cases. The compactor suite passes. This adds the changed upstream coverage to the original 376-target check. Tempo/Grafana readiness remains under investigation: the response-body diagnostic passed three times, so no failure body is captured yet. The original repeated failures remain preserved.

The [Pyroscope fingerprint confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37365605239) repeats the same preserved images and three alternating revision pairs. It checks the first control's mixed query and ingest latency before a global retention decision.

The [Mimir fingerprint confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37363580969) passes 725 raw checksum checks. CPU, RSS, query and ingest median ratios are 0.9758, 0.9914, 1.0790 and 1.0245. Only two pairs have lower CPU, and one has lower query or ingest p99. RSS is lower in all three pairs with disjoint ranges, reversing the first control's direction. Across both controls, only three of six pairs have lower total CPU or query p99. No repeated Mimir resource or query gain is qualified. Pyroscope confirmation continues.

The response assertion now preserves a failed Grafana echo response body. Three fresh full current and three previous Tempo wrappers pass with retries disabled, each executing six cases without ignored cases. Clippy passes. A temporary no-delay diagnostic also passes; its source is restored. None of these runs reproduce the failure body or prove a cause. The original 404, retry and two failed unmodified follow-up attempts remain preserved. The checked metadata correctness scope is qualified with this explicit limitation; no test setup fix or performance retention is claimed.

The branch now includes main `92989bb2`, including Tempo's cold tag-scope correction. All 120 branch patches survived the rebase unchanged. Tempo's unit suite, two Clippy checks and all 12 deployment cases pass with retries disabled.

The [Pyroscope fingerprint confirmation](https://github.com/krabka-io/krabka-o11y/actions/runs/37365605239) passes 743 raw checksum checks. Median candidate/baseline CPU, RSS, query and ingest ratios are 1.0905, 0.9962, 1.1615 and 0.9337. CPU is higher in two pairs and query p99 in all three; ranges overlap. Together with the inconsistent Mimir controls, this does not qualify a reliable shared-function advantage. The fold is rejected. The original loop is restored byte for byte, both stronger golden vectors remain, and all 22 cross-signal core unit, canonical golden and Clippy targets pass.

The cold fingerprint reuse candidate preserves the canonical-keyed map already built during label deduplication. Instant label resolution and cardinality avoid hashing those labels again. The regression uses duplicate labels under differing float and histogram index IDs and still expects canonical keys, complete labels and retained ownership. All 56 ordinary PromQL and metrics-service checks pass, as do the real Mimir and Prometheus differential wrappers and all 12 deployed metrics cases, with no ignored native cases or retries. These checks precede the fingerprint-loop restoration; the subsequent 22 core checks cover the combined source. No performance gain is qualified. See `qualification/grafana-cold-fingerprint-reuse-experiment-gcp.json`.

Both dense metadata image/profile runs are verified: 234 baseline and 238 candidate raw checksums. Runtime configuration, architecture, operating system, base layers and all nonbinary filesystem entries match across 380 paths. Only the seven application executables differ. The matched Mimir measurement follows using those exact images. Both images contain the same subsequently rejected fingerprint fold, so the comparison isolates dense metadata and does not qualify the restored branch or the new reuse candidate.

The [dense metadata Mimir comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37370310006) passes 725 raw checksum checks. Candidate/baseline median CPU, RSS, query and ingest ratios are 1.0097, 1.0034, 0.9808 and 0.9351. Total CPU is higher in all three pairs; RSS and querier CPU are higher in two. Query and ingest p99 are lower in two pairs, with overlapping ranges. There is no localized resource advantage supporting confirmation. The candidate is rejected, its full metadata ledger remains, and the original production function is restored byte for byte. All seven metadata, blockstore, PromQL, metrics-service and Clippy restoration checks pass.

Both restored-fingerprint reuse image captures pass 238 raw checksum checks. All runtime settings and nonbinary filesystem entries match across 380 paths. Only the metrics-service executable differs. The matched Mimir comparison follows; both images retain equal rejected dense metadata, so the control isolates reuse and does not qualify the final restored source. The original-fingerprint combined source also passes all 14 native Mimir/Prometheus cases with retries disabled.

The separate `codex/grafana-single-cgu` reference changes only the existing Rust code generation unit setting to one. Source and dependency versions match the verified reuse candidate image used as its baseline. Bazel action analysis confirms the compiler argument, and both canonical fingerprint golden targets pass under that setting. Its image build and diagnostic Loki capture run on private GCP. This is a compiler experiment: neither the golden checks nor static image size qualify a runtime CPU or RSS advantage, and broader correctness checks and matched measurements remain required before retention. See `qualification/grafana-single-cgu-release-experiment-gcp.json`.


The cold-fingerprint reuse [repeat](https://github.com/krabka-io/krabka-o11y/actions/runs/37377663184) verified 725 raw checksums. Both revisions accept 120,000 rows and complete 240 queries per pair. Candidate/baseline median CPU, RSS, query p99 and ingest p99 ratios are 0.9862, 1.0019, 0.9437 and 0.9860. Query p99 is lower in all three repeat pairs and five of six pairs across both controls; all ranges overlap. The first RSS median gain does not repeat, and CPU is lower in only two of six pairs. This remains a query candidate pending a control on final source, with no qualified CPU or RSS gain.

The single-codegen-unit cross-signal controls now qualify narrower findings. [Loki](https://github.com/krabka-io/krabka-o11y/actions/runs/37380543910) verifies 725 raw checksums and equal 239-query counts per revision. RSS falls in all three pairs with disjoint ranges (median ratio 0.9269), but query p99 rises in all three pairs with disjoint ranges (1.1082). CPU and ingest results are mixed. [Tempo](https://github.com/krabka-io/krabka-o11y/actions/runs/37380619358) also verifies 725 raw checksums and equal query counts. RSS falls in all three pairs with disjoint ranges (0.9510); CPU, query and ingest changes remain mixed. These preserved-image controls do not qualify current HEAD. The compiler setting stays off on the performance branch: the Loki query regression prevents global retention on this evidence.

Compiler correctness covers 23 core checks and ten native wrappers, with 67 native cases and zero ignored cases. The full 385-target ordinary suite stops on a Rust E0275 trait-recursion build error in `//crates/integration:backup_restore_test`, after 80 targets pass. The same error reproduces with the default compiler configuration. The failure logs are preserved. The test crate now uses the same trait recursion depth as the other PromQL router suites; its test and Clippy check pass with three cases and zero ignored cases. This is a failed full build, not a full correctness pass. Exact scopes and proof hashes are in `qualification/grafana-single-cgu-release-experiment-gcp.json`.


The final [Pyroscope compiler control](https://github.com/krabka-io/krabka-o11y/actions/runs/37380853989) verifies 743 raw checksums. Its candidate/baseline median CPU, RSS, query p99 and ingest p99 ratios are 0.9956, 0.9548, 0.9642 and 1.0223. RSS falls in all three pairs with disjoint ranges; the other metrics remain mixed. All four exact-image controls are preserved. The global compiler setting is not retained because of the Loki query regression. A metrics-specific setting requires its own exact-image comparison on final source.

The [pinned upstream source audit](grafana-upstream-source-comparison.md) separates measured publication costs from optional caching, and records missing streaming, projection, postings and symbol-reuse techniques with their correctness guards. Its first candidate reduces repeated compactor manifest reads while preserving current publication timing and destructive-reader validation.


After the backup/restore test recursion repair, all 385 ordinary Bazel targets pass with one code generation unit and retries disabled. Ignored Rust container cases are catalogued in the full-scope proof; the separately executed ten native wrappers contain 67 cases and zero ignored cases. This completes the ordinary scope on source `502ab8f4`, not qualification of the historical compiler image or of a new cache candidate. The global setting remains off because of the Loki query regression.
