# Local Grafana performance optimization

These measurements evaluate the optimizations after the issue 267 comparison,
using the [reusable harness](grafana-performance-comparison.md#provenance-and-reproduction).
They use the same request payloads and deployment budgets on a local AMD EPYC
4344P host with 16 logical CPUs. They do not replace the historical GCP results
or establish equivalent durable throughput.

## Changes

Mimir work removes the SQL window sort from hot/cold metric deduplication,
keeps footer caches across moving query windows, avoids repeated hot-series
matching and sorting, and shares labels between retained samples. Hot samples
still win over cold samples, including stale markers and native histograms.
Instant queries keep the last sample only after checking the complete scan
against the sample limit; range queries retain their complete windows.

Loki work rejects nonmatching rows before cloning them, parses response sort
keys once, applies folded response limits before JSON allocation, and limits
hot-tail statistics work to timestamps present in the returned response.
Interval and categorized-label queries retain their existing later transforms.

Tempo work packs live spans from separate traces into Arrow batches of at most
8,192 rows. Each trace keeps its own root metadata and nested-set calculations,
including when the query window excludes its root span. This avoids encoding
and decoding hundreds of small batches per query.

## Measurement contract

The baseline is the branch's starting release image, source
`9864c6a93b4c53f5e71e83161999f7f2349d9199`. Each measured backend has three fresh
deployments, 15 seconds of warm-up, and 60 seconds of measurement. The candidate
and native backend alternate order. Only the metrics baseline also has paired
native controls; logs and traces baseline runs measure Krabka alone.

CPU includes all application roles, the broker where present, and MinIO. Peak
RSS is the simultaneous sum across these containers. Queries are closed-loop
at up to four requests/s, so completed counts are retained with the results.
No builds, tests, or CPU profiles ran during these measurements. Diagnostic
profiles used to choose the changes are excluded from the result set.

The compact [local evidence record](../qualification/grafana-performance-local.json)
contains image and binary identities, harness hashes, report checksums, all
three measurements, telemetry coverage, and the remaining native ratios.
Raw reports, Compose configurations, logs, and telemetry stay under
`qualification/evidence/performance/`, with a `SHA256SUMS` for each run.

These are steady-load results. The writer and cardinality ramps have not been
rerun for this candidate, and this local run has no remote CI qualification.

## Results

Values are medians of three measurements. CPU is container CPU seconds during
each measured minute; RSS is MiB. The baseline is the current branch's starting
image, rather than the older image in the historical GCP table.

| Signal | Query p99 ms, before → after | CPU seconds, before → after | Peak RSS MiB, before → after |
| --- | ---: | ---: | ---: |
| metrics | 207.53 → 53.33 | 36.61 → 13.06 | 671.42 → 514.45 |
| logs | 53.43 → 49.99 | 10.02 → 9.68 | 400.99 → 407.54 |
| traces | 98.91 → 44.75 | 23.69 → 11.36 | 701.95 → 557.35 |

| Candidate / native median | Average container CPU | Peak RSS | Query p99 |
| --- | ---: | ---: | ---: |
| metrics / Mimir | 1.80× | 1.69× | 5.55× (53.33 / 9.61 ms) |
| logs / Loki | 1.04× | 1.12× | 1.22× (49.99 / 40.86 ms) |
| traces / Tempo | 0.77× | 1.05× | 0.89× (44.75 / 50.17 ms) |

Mimir work reduces CPU by 64%, RSS by 23%, and p99 by 74%, but leaves a material
gap against native Mimir. Loki p99 falls 6% and CPU falls 3%; its RSS is 2%
higher, so this run establishes no memory improvement. Tempo p99 falls 55%,
CPU falls 52%, and RSS falls 21%. It beats native Tempo on CPU and query p99 in
this workload, while using 5% more RSS. Three repetitions do not provide a
confidence interval for small differences.

Every measured entry met the steady API objectives, with zero ingest errors,
query errors, and empty query responses. Metrics and logs complete 240 queries
per repetition on both backends. Tempo completes 238 on Krabka and 237 on
native Tempo; the trace baseline completes 238. All repetitions have at
least 90% resource sampling coverage. Candidate p99 ranges are 53.22–53.67 ms
for metrics, 49.26–51.07 ms for logs, and 44.53–45.36 ms for traces.

Metrics measurements use source `1be32daac1ba715fab4a8e4be3f50d2b8589087c`.
Logs and traces use the final source `23e8b1ed7162055b86974064be52d450ef3cdbf5`.
The final metrics and observability binaries were compared byte for byte with
the binaries in the measured metrics image and are identical. Image manifests
and binary SHA-256 values are preserved in the evidence record.
The metrics baseline predates automatic harness hashing; its report therefore
has no harness file hashes, and its raw checksums were recorded after the run.

## Validation

Scoped PromQL, metrics service, LogQL, observability, and traces tests and
Clippy targets passed. Real Prometheus, Mimir, Loki, and Tempo differential
Docker suites passed. The Tempo suite first needed an automatic retry because
Grafana's newly created datasource proxy returned 404 on `/api/echo`; a fresh
run with retries disabled passed all six tests. The direct Tempo comparisons
passed on both attempts. Harness self-tests cover encoding and resource
accounting; fresh summaries verify raw file checksums and telemetry coverage.
