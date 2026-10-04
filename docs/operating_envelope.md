# Operating envelope

Krabka does not publish a numeric production limit until the complete workload
has reached saturation on the stable qualification runner. Configured tenant
limits are safety controls, not performance claims.

## Fixed qualification shape

The stable qualification uses the broker-backed Compose deployment described
below. Its report fixes the role CPU and memory limits, broker and object-store
digests, WAL partitions, retention, replication, dataset, warm-up, measurement
duration, and actual runner hardware. It records the measured image source
commit separately from the harness commit. The checked-in Kubernetes topology
and its HA deployment are separate shapes that need their own qualification.

## Broker-backed deployment qualification

The `operating envelope` workflow uses the installed
[Google Cloud runner manager](https://github.com/Cyclenerd/google-cloud-github-runner).
Its label is `gcp-ubuntu-24-04-16core`. The manager matches that label to the
regional `e2-standard-16` template in `robot-head`, `us-central1-b`.
The template has 16 vCPUs, 64 GiB RAM, and a 600 GB SSD boot disk.
The workflow records the actual machine type, image, CPU model, kernel, and Docker configuration.
The label uses dashes because GCE refuses a dot in a VM label value.

The default `deployment` job uses `tools/deployment-envelope.py` and the
checked-in Compose topology. Each signal has one distributor, block builder,
and querier. Metrics, traces, and profiles also have one compactor; traces
have a separate live store. Every role has a one-vCPU, one-GiB limit. The
broker and MinIO each have two-vCPU, two-GiB limits. Every container has a
65,536-file descriptor limit. The WAL has one partition, replication factor
one, and 900-second retention. Blocks are retained without an age limit,
except that the deletion phase enables 60-second retention on tenant
`expired`. Maintenance runs every two seconds. Block flushes have a
two-second maximum age; the traces poll window is five seconds. Profiles and
traces use a 30-second hot retention window, and metrics use five minutes.
Rates apply to one active signal at a time, with the other signal roles idle.
The resolved Compose file and all role configurations are raw artifacts.
This shape qualifies direct querier APIs on one node; it does not qualify
query frontends, replication, failover, or an external object-store provider.

The HTTP dataset uses seed `267`. Metrics and logs send 100 series with ten
points per request. Traces send ten series with ten spans per request.
Profiles send one labelled profile with ten stack samples. A fresh tenant is
used at each load level. The cardinality search loads every label value before
measurement, and retains seed requests as JSON Lines. Ingest caps are disabled
on the capacity tenants, and the logs querier has no series-count cap. A
failed higher load, including an OOM during preload, remains failure evidence.
The deployment is reset before subsequent phases use the last passing corpus.
Ramp steps run in the stated order. Earlier tenants and their hot data remain
resident until the reset before `cold_blocks`, except when an overloaded
burst kills a role and forces an earlier reset. The published ramp limits
include that accumulated data and tenant load.

Each full phase measures 60 seconds after a 15-second warm-up. Burst and
cardinality steps measure 30 seconds after 7.5 seconds of warm-up. Writers at
burst levels 1 through 512 send one request per second each. Cardinality
steps are 100, 1,000, 5,000, 20,000, 100,000, and 500,000, with two writers
sending one request per second each. Steady, compaction, deletion, and quiet
tenant loads also use two writers sending one request per second each. The
search stops at its first failure. The
write and query p99 objectives are two seconds, with zero errors and no empty
query results. Accepted writes must become durable within ten seconds after
the phase. The reported durable rate includes that drain time.
One reader sends queries every 250 ms, waiting for each response. Burst limits
use 100 label sets; cardinality limits use the two paced writers. They measure
separate loads. The cold corpus must have exactly the published cardinality
in all three runs, and future gates keep that corpus fixed.

| Signal | Query |
| --- | --- |
| Metrics | Instant `sum(envelope_samples)`; cold reads use `sum(last_over_time(envelope_samples[30m]))` |
| Logs | Loki range query `{job="envelope"}`, at most 1,000 lines |
| Traces | Tempo search `{resource.service.name="envelope"}`, at most 1,000 traces |
| Profiles | `SelectMergeStacktraces` for `{service_name="envelope"}`, type `process_cpu:cpu:nanoseconds:cpu:nanoseconds`, at most 1,024 flamegraph nodes |

Logs, traces, and profiles read a 30-second hot window and a 30-minute cold
window. Cardinality steps also use the wide window. HTTP requests carry the
tenant in `X-Scope-OrgID`; listeners are plain HTTP on the runner's loopback.

The `cold_blocks` phase reads a ten-minute-old corpus over a 30-minute window,
with no concurrent writes to that tenant. It must transfer object bytes. This
measures block reads; it does not promise a cold operating-system cache. The
`compaction` phase requires observed output blocks and no maintenance errors
while writes and queries run. The `deletion` phase first verifies an aged
corpus exists, then enables retention during the measured round and verifies
that it disappears while the control tenant keeps writing and querying.
The noisy phase runs four unpaced writers beside two paced writers on
`quiet`. Noisy limits are 2,000 rows or spans per second for metrics and
traces, 20 profiles per second, and a broker `producer_byte_rate` quota of
65,536 bytes per second for logs.

The report records query p50/p95/p99, HTTP status counts and errors, accepted
and durable ingest rates, RSS by role including broker and MinIO,
object requests and bytes, broker-reported WAL lag, and consumer recovery
status. CPU time is included when the process exports it. Restart stops the
block builder, appends ten batches, and checks that
the durable offset advances through those records after restart. Every drain
reads the broker's current WAL end offset and requires the consumed and
committed offsets to reach it. Every raw operation and telemetry sample,
runner identity, container log, source commit,
image digest, and checksum is retained. A locally built image is preserved as
a checksummed `deployment-image-<commit>` artifact, including its manifest and
Docker image IDs. `image_artifact_run` reuses that exact build.
During the deliberate retention-owner restart, sampling continues for every
other role. Samples record `expected_restart_gaps` for the owner until its
new PID and metrics endpoint are ready. Unexpected telemetry failures remain
qualification errors. RSS peaks include roles missing from the final sample;
the sum of per-role peaks is not a simultaneous process-memory measurement.

Three complete comparable runs support `qualification/deployment-envelope-baseline.json`.
The gate publishes the highest passing load common to all three runs only
when a higher load actually fails. It records median, range, and coefficient
of variation for each metric. Future runs must retain the published workload
and host shape, pass every full phase, sustain the published load, and remain
within a 1.5x regression tolerance. Missing evidence or an unseeded baseline
fails. The self-test includes negative controls for throughput, query latency,
RSS, missing maintenance, absent expiry, empty queries, and duplicated runs.

Use `scope=checks` for the complete ordinary Bazel test suite on GCP. Use a
short phase, one repetition, and `record=true` for diagnostics. Use
`scope=image` to build and preserve an optimized image before measurement.
`scope=storage` retains the
native four-worker storage soak as a separate component diagnostic; its WAL
lag is explicitly unmeasured. Its complete qualification, if recorded, uses
three optimized runs and the median of each metric, with a separate native
workload reference. The deployment and component reports describe different
workloads and are not interchangeable.

For a fresh baseline, dispatch the workflow with `scope=deployment`,
`record=true`, `phase_seconds=60`, and `repetitions=3`. Review the three reports
and their `deployment-baseline.json` before replacing the checked-in baseline.
Dispatch with `record=false` to apply the stable-runner regression gate.

## Supported envelope

Pending stable-runner qualification. The table below stays empty until
`tools/deployment-envelope.py --reports` accepts three complete, comparable
reports and observes a passing load followed by saturation for every signal.

| Signal | Burst writers | Durable rows for each second | Active series | Runs | Image commit |
| --- | --- | --- | --- | --- | --- |
| Metrics | Pending | Pending | Pending | 0 | None |
| Logs | Pending | Pending | Pending | 0 | None |
| Traces | Pending | Pending | Pending | 0 | None |
| Profiles | Pending | Pending | Pending | 0 | None |

Until the stable-runner report covers every workload and all four signals,
the supported numeric envelope remains unpublished. Raw shared-runner results
are diagnostic evidence, not a capacity promise.

## Native storage component workload

The soak harness is `//crates/integration:soak_envelope_docker_test`. It runs
each signal's real block writer, compactor, retention pass, and query engine in
one test process, on four Tokio worker threads, against one MinIO container.
It does not start a broker or the Kubernetes topology. Each signal writes
blocks straight to the object store, so the harness measures the storage and
query path, and it reports WAL lag as `not_measured`.

The dataset is fixed by the seed `267`, which `KRABKA_SOAK_SEED` can override.
Every value comes from a SplitMix64 hash of the seed, the batch sequence number,
the series, and the point. Timestamps are the wall clock at write time, so the
retention and query windows are real. One batch holds 100 series of 10 points
for each signal:

| Signal | One series | One point |
| --- | --- | --- |
| Metrics | A float series on `soak_samples` with a `series` label | One sample |
| Logs | A stream under `app="api"`, `env="prod"` with a `series` label | One log line |
| Traces | One trace with `service.name=api` | One span |
| Profiles | One `process_cpu` profile with `service_name=api` | One sample |

Three tenants write: `soak` for most phases, and `quiet` and `noisy` for the
noisy-tenant phase. The report records the dataset and the shape in its
`dataset` and `shape` fields.

The workload must run steady ingest, a burst to saturation, increasing label
cardinality, hot and cold queries, compaction, deletion, restart catch-up, and
one noisy tenant beside a quiet tenant for metrics, logs, traces, and profiles.
For each phase and signal it records accepted and rejected ingest rate,
p50/p95/p99 query latency, errors, RSS, WAL lag, object-store requests and
bytes, and recovery time. The first supported point is the highest load below
which three complete runs remain inside the error and latency objectives; a
configured maximum or a partial run is never promoted.

## Evidence and gates

The scheduled scale soak writes `soak-report.json`, schema version 2, with one
entry for each signal and phase. The sections below give its phases, its
fields, and the gate that reads it. Criterion
writes raw estimates, confidence intervals, runner metadata, duration, and
`SHA256SUMS` below `benches/target/criterion`. Manual mutation sweeps preserve every
shard log plus commit, toolchain, host, command, duration, and checksums.

Object-store cost is counted in requests and bytes. `MeteredObjectStore` in
`krabka-blockstore` counts each request attempt and its payload bytes per
operation. A report writes those counts as the `operations` and
`transferred_bytes` maps, plus their sums as `requests_total` and
`transferred_bytes_total`. The `object-store contract` workflow writes these
fields for the provider suite and for one lifecycle test per signal.
`tools/object-store-evidence.py` rejects a report whose totals are zero or
differ from the sums of their maps. See
[`object_store_contract.md`](object_store_contract.md).

`tools/bench-ratchet.py` rejects missing or noisy measurements and applies
numeric baselines only after a quiet stable runner produces them.
`tools/mutants-ratchet.py` rejects missing, silent, timed-out, or internally
inconsistent shards before comparing survivor counts. A deliberate benchmark
regression or new mutation survivor must fail before either baseline is
reviewed. `tools/mutants-record.py --check` runs on every pull request. It
fails when a baseline number has no recorded run behind it, or when the record
does not name its commit, toolchain, host shape, command, duration, and
checksums.

### Soak phases

Each signal runs eight phases in this order. The default phase is 12 seconds,
and `KRABKA_SOAK_PHASE_SECONDS` changes it. Each phase starts with a warm-up of
one quarter of the phase, and the harness discards the measurements from the
warm-up. The warm-up and the measured window are two rounds of the same load.
In each round, no loop starts an operation after the round ends, but an
operation in progress runs to its end. The harness waits for the last warm-up
operation to end. Then it reads the object-store counters and starts the
measured round. Thus each operation is in one round only, and its requests,
its latency, and its count are all in that round. The measured window extends
to the last operation, so a slow query is measured whole and is not dropped.
The warm-up also extends to its last operation, and `warmup_seconds` gives
that full time.

| Phase | Load |
| --- | --- |
| `steady` | Two paced writers, one batch each 250 ms, and two readers on one tenant |
| `burst` | Unpaced writers at 1, 2, 4, and 8, for half a phase each |
| `high_cardinality` | One paced writer at 100, 1,000, and 5,000 series for each batch, for half a phase each |
| `cold_blocks` | No writes. Each query reloads the index and reads the full 30-minute window |
| `compaction` | The steady load, with a compaction pass each 2 seconds |
| `deletion` | The steady load, with a 60-second retention pass each 2 seconds |
| `noisy_tenant` | The steady load on `quiet`, beside four unpaced writers on `noisy` |
| `restart` | A cold open over the same store, timed to the first query that returns data |

The `burst` and `high_cardinality` phases stop at the first level that misses
an objective. The objectives are a write p99 and a query p99 of 2 seconds
each, and an error rate of zero. A phase that drives a path with no successful
operation on it has no p99, and it does not meet the objectives. `KRABKA_SOAK_WRITE_P99_MS` and
`KRABKA_SOAK_QUERY_P99_MS` change the latency objectives. The order is part of
the shape: `cold_blocks` reads the blocks that the three phases before it
wrote, and `deletion` then removes them.

In `noisy_tenant`, the metrics and traces paths put the real per-tenant
ingest limiter in front of `noisy`, at 2,000 rows for each second. The logs
and profiles limiters are outside the path that the harness drives, so their
entries report `rate_limiter.status` as `not_applied` and no rejected rows.

### Report fields

The top level of the report names the run:

- `run_id`, from `KRABKA_SOAK_RUN_ID`. When that is unset, the harness makes
  one from the commit, the start time, and the process ID, so each run has its
  own
- `commit`, from `KRABKA_SOAK_COMMIT`, and `image_digest`, from
  `KRABKA_SOAK_IMAGE_DIGEST`
- `minio_image`, with the reference and the local image ID
- `host`, with the CPU count, memory, OS, architecture, and
  `KRABKA_SOAK_RUNNER`
- `rustc`, `command`, `total_seconds`, `dataset`, `shape`, and `objectives`

Each entry in `entries` has a `signal` and a `phase`, and these fields:

| Field | Content |
| --- | --- |
| `warmup_seconds`, `duration_seconds` | The discarded warm-up, until its last operation ended, and the measured window |
| `ingest` | Attempted, accepted, and rejected batches and rows, accepted and rejected rows for each second, and `latency_us` |
| `query` | Count, errors, queries for each second, rows seen, and `latency_us` |
| `latency_us` | Count, p50, p95, p99, and max in microseconds, and `cv` |
| `error_rate` | Errors over attempts. A rate-limiter rejection is not an error |
| `objectives_met` | Whether the entry met all three objectives |
| `tenants` | The `ingest` and `query` fields for each tenant that the phase drove |
| `rss_kib` | Process resident memory at the start, the peak, and the end |
| `wal_lag` | Always `not_measured`, with the reason |
| `object_store` | For the write, read, and maintenance paths: requests and bytes by operation, and requests and bytes for each operation |
| `maintenance` | Passes, errors, and latency of the compaction or retention loop |
| `recovery_seconds`, `recovery` | For `restart` only: the time to the first query that returned data |
| `saturation`, `levels` | For the stepped phases only: the highest level that met the objectives, the first level that did not, and each level's numbers |

The `cv` field is the coefficient of variation of the mean latency across
5-second windows. It is null when the measured window holds fewer than two
windows.

### Soak gate

`tools/soak-gate.py` reads the report. Each mode applies a structural check
first. The check fails a report that is not schema version 2, or that is
without an entry, a field, a quantile, a saturation search, or a recovery
result. It also fails when a steady phase accepted no batch. The scheduled
`scale` job runs this check only, with `--structural`, because a shared runner
gives different numbers on different hosts.

Without `--structural`, the tool compares the report with
`tools/soak-baseline.txt`, and fails a metric that moves against its baseline
by more than `--tolerance`. The default tolerance is 1.5. The gated metrics
are the write and query p99, the accepted rows for each second, the peak
resident memory, the write and read requests for each operation, and the
recovery time. For `restart`, the read requests for each operation are the
object-store reads for each recovery attempt. When the latency `cv` is above
`--noise-ceiling` or null, the tool skips the latency and throughput verdicts
for that entry and prints why.
It never skips the memory, request, or recovery verdicts. The baseline is also
the inventory: a baseline line without a value, and a value without a
baseline line, both fail. Every line reads `unseeded` today, so only the
inventory applies.

### Envelope computation

`tools/soak-gate.py --envelope` reads three or more reports. Each report must
have its own `run_id`, so one run cannot count as two. The reports must also
have the same dataset, shape, and objectives, and the same build and
platform. The build is the `commit`, `image_digest`, `minio_image`, and
`rustc`. The platform is the full `host` object. The tool refuses reports
that differ, and it names the fields that differ.

For each signal, the tool gives the highest burst level and the highest
cardinality step at which every run met every objective. It also gives the
minimum, mean, and CV of the accepted rows for each second at that level.
When no run failed at the top level, the result is a lower bound, and the
tool says so. Only a `saturated` result can become a published limit.

### Native stable-runner qualification

Run the soak three times on the stable runner, with the commit and runner
recorded, and keep each report:

```bash
for run in 1 2 3; do
  bazel test --config=scale --cache_test_results=no \
    --test_env=KRABKA_SOAK_COMMIT="$(git rev-parse HEAD)" \
    --test_env=KRABKA_SOAK_RUNNER=<runner> \
    --test_env=KRABKA_SOAK_PHASE_SECONDS=60 \
    //crates/integration:soak_envelope_docker_test
  cp bazel-testlogs/crates/integration/soak_envelope_docker_test/test.outputs/soak-report.json \
    "soak-report-${run}.json"
done
tools/soak-gate.py --envelope soak-report-1.json soak-report-2.json soak-report-3.json
tools/soak-gate.py --record soak-report-{1,2,3}.json > tools/soak-baseline.txt.new
```

Bazel can zip the test outputs into `outputs.zip`. If it does, extract
`soak-report.json` from that file first.
