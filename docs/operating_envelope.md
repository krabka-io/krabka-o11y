# Operating envelope

Krabka does not publish a numeric production limit until the complete workload
has reached saturation on the stable qualification runner. Configured tenant
limits are safety controls, not performance claims.

## Fixed qualification shape

The candidate deployment is the checked-in Kubernetes topology: one replica
per role, one broker, one object store, one partition per WAL/state topic,
replication factor one, 15-minute WAL retention, and the CPU/memory requests
and limits in `deploy/kustomization.yaml`. The HA phase renders two WAL
partitions so two block builders and two queriers per signal can own work; it
leaves state topics and replication at one. A report is comparable only when it
names the exact Krabka and broker commits, container digests, Kubernetes
version, node CPU and memory, object-store provider, retention, replication,
dataset seed, warm-up, measurement duration, and command.

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
warm-up. The harness counts each operation that ends after the warm-up. No
loop starts an operation after the phase ends, but an operation in progress
runs to its end. The measured window then extends to the last operation, so a
slow query is measured whole and is not dropped.

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
| `warmup_seconds`, `duration_seconds` | The discarded warm-up and the measured window |
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

### Stable-runner qualification

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
tools/soak-gate.py --record soak-report-1.json > tools/soak-baseline.txt.new
```

Bazel can zip the test outputs into `outputs.zip`. If it does, extract
`soak-report.json` from that file first.

## Supported envelope

Pending stable-runner qualification. The table below stays empty until
`tools/soak-gate.py --envelope` gives a `saturated` result for each signal
from three stable-runner reports.

| Signal | Burst writers | Accepted rows for each second | Series for each batch | Runs | Commit |
| --- | --- | --- | --- | --- | --- |
| Metrics | Pending | Pending | Pending | 0 | None |
| Logs | Pending | Pending | Pending | 0 | None |
| Traces | Pending | Pending | Pending | 0 | None |
| Profiles | Pending | Pending | Pending | 0 | None |

Until the stable-runner report covers every workload and all four signals,
the supported numeric envelope remains unpublished. Raw shared-runner results
are diagnostic evidence, not a capacity promise.
