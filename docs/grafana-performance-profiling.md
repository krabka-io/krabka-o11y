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
software `cpu-clock:u` event at 99Hz with DWARF call stacks. This works without
a virtual hardware performance counter. Reports resolve symbols through each
live process's container root; flat and cumulative reports accompany
the raw perf data. Some recorded mappings could not be unwound completely,
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
filtering and output lifetime. Performance qualification is pending.

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

The borrowed-selection image `ca99cacd` has verified [CPU](https://github.com/krabka-io/krabka-o11y/actions/runs/37299759203) and [allocation](https://github.com/krabka-io/krabka-o11y/actions/runs/37299762062) captures. Archive digests and all 162 and 130 raw checksums pass. The whole-span clone is absent from the allocation summary. It records 73,220,999 allocations and 7,010,365 string clones over 104.90 seconds including startup and seed. Both this pass and the preceding pass accept 14,000 spans on AMD EPYC 7B12, but this pass completes 147 queries versus 138. These totals are not a per-operation comparison; peak Rust heap rises from 35.16 to 41.84MB. The same-VM uninstrumented comparison remains the retention gate.

Independent image checks show that only `krabka-traces` differs from `8541b68f`; the other six application binaries, runtime configuration, base layer and all remaining filesystem entries match. All 380 entries were compared. This equivalence does not establish a performance result for the new traces binary.

The Pyroscope fingerprint experiment on `913fa01b` hashes borrowed label pairs in both the sample store and WAL record. A temporary ordered map retains sorted names and the last value for duplicate names; it no longer copies the strings solely to hash them. Owned labels and borrowed pairs share the original FNV-1a encoder with little-endian byte lengths. The input label vectors, sample fields and WAL format are preserved.

Six fixed hash vectors cover empty inputs, sorting, duplicate names, Unicode byte lengths, NULs and delimiter ambiguity. Production sample insertion and WAL record regressions check canonical hashes and complete payloads. Changing duplicate handling to first-value wins fails all three regressions while the other 121 pprof unit cases pass. All 79 scoped checks pass with the correct code, including the real Pyroscope suites with both Grafana versions, Mimir, Prometheus and Clippy.

Its [same-VM revision comparison](https://github.com/krabka-io/krabka-o11y/actions/runs/37301689980) uses the verified `ca99cacd` image as baseline. Performance qualification and candidate profiles are pending; the source change alone is not a measured gain.
