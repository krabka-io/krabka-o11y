# krabka-o11y Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Project** | `krabka-o11y` |
| **Signals** | metrics, logs, traces, profiles |
| **Date** | 2026-09-13 |

These counts describe the dated snapshot. The newer [query-language qualification](query_language_conformance_proposal.md) includes the full Prometheus 3.14 corpus, pinned HTTP runners and individual semantic verdicts; a passing regression target does not certify every upstream feature.

## Crate Reports

| Crate | Signal | Representative status | Report |
| :--- | :--- | :--- | :--- |
| `krabka-blockstore` | all | Pass | [Report](../crates/blockstore/test_coverage_report.md) |
| `krabka-integration` | cross-signal | Pass | [Report](../crates/integration/test_coverage_report.md) |
| `krabka-logql` | logs | Pass | [Report](../crates/logql/test_coverage_report.md) |
| `krabka-metrics` | metrics | Pass | [Report](../crates/metrics/test_coverage_report.md) |
| `krabka-metrics-service` | metrics | Pass, including differential | [Report](../crates/metrics-service/test_coverage_report.md) |
| `krabka-observability` | logs | Pass, including differential | [Report](../crates/observability/test_coverage_report.md) |
| `krabka-pprof` | profiles | Pass | [Report](../crates/pprof/test_coverage_report.md) |
| `krabka-profiles` | profiles | Pass, including differential | [Report](../crates/profiles/test_coverage_report.md) |
| `krabka-promql` | metrics | Pass, full vendored corpus | [Report](../crates/promql/test_coverage_report.md) |
| `krabka-query-frontend` | all | Pass | [Report](../crates/query-frontend/test_coverage_report.md) |
| `krabka-traceql` | traces | Pass, golden corpus | [Report](../crates/traceql/test_coverage_report.md) |
| `krabka-traces` | traces | Pass, including differential | [Report](../crates/traces/test_coverage_report.md) |

## Compatibility Coverage

| Signal | Executable authority | Status |
| :--- | :--- | :--- |
| Metrics | Prometheus and Mimir differential suites; PromQL corpus | Pass |
| Logs | Loki and Grafana differential suites | Pass |
| Traces | Tempo and Grafana differential suites; TraceQL golden corpus | Pass |
| Profiles | Pyroscope and Grafana differential suites | Pass |

The [API compatibility matrix](api_compatibility.md) limits each statement to the routes and behaviors those suites exercise.

## Test Inventory

The per-crate reports account for 4,458 source-declared tests across unit, property, corpus, integration, and differential layers.

Fourteen fuzz targets cover protocol decoders and query parsers.

Nine Docker-tagged suites compare Krabka with pinned upstream and Grafana images.

## Line Coverage

CI runs `bazel coverage //crates/...` and uploads the workspace LCOV file to Codecov.

Each crate report gives the scoped `cargo llvm-cov nextest` command used to obtain a per-crate percentage.

No percentage is quoted here without the complete LCOV output for this revision.

## Mutation Coverage

Twelve crates define mutation targets. Sweeps run by hand, not in CI. Eight have reviewed
baselines from complete, checksum-verified runs: blockstore (145 survivors),
logql (91), metrics (174), metrics-service (119), pprof (86), query-frontend
(64), traceql (1), and verified (2). The run metadata and artifact checksums are
recorded in
[`qualification/milestone-19-mutation-baselines.json`](../qualification/milestone-19-mutation-baselines.json),
and `tools/mutants-record.py --check` makes sure that each baseline number
matches its record.

The other four targets, observability, profiles, promql, and traces, remain
`unseeded` until every shard completes on a dedicated host and
`tools/mutants-ratchet.py` validates the totals. The procedure is in
[`README.md`](../README.md#seeding-a-crate-by-hand).

## Cross-Cutting Gaps

Ignored differential suites require Docker and are not part of scoped line-coverage commands.

The metrics collection tier, trace sampling, Jaeger queries, and legacy Pyroscope label routes are deliberate product boundaries rather than missing tests.

## Conclusion

Every workspace crate now has a report, and every supported upstream surface in the compatibility matrix points to a live differential suite.

CI remains the authority for pass status and line coverage. Complete mutation output comes from manual sweeps.
