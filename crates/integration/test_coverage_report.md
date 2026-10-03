# krabka-integration Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-integration` |
| **Signal** | cross-signal |
| **Upstream surface** | Logs-to-traces, traces-to-metrics, and traces-to-profiles links |
| **Date** | 2026-09-13 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **Logs-to-traces, traces-to-metrics, and traces-to-profiles links** | A trace ID on a log resolves the trace | Pass | `tests/logs_to_traces_trace_id.rs::a_trace_id_on_a_log_line_fetches_the_trace` | Cross-crate integration |
| **Deployment backup and restore** | An empty deployment restores every tenant and signal from one cut, and ingest resumes with no loss or duplicate | Pass | `tests/backup_restore.rs::an_empty_deployment_restores_every_tenant_and_signal_and_resumes_ingest` | In-process broker and query equivalence |
| **Deployment backup and restore** | A broker restored from another time is refused before a write | Pass | `tests/backup_restore.rs::a_broker_restored_from_another_time_is_refused_before_any_write` | In-process broker |
| **Deployment backup and restore** | A restored deployment keeps a Prometheus TSDB import, and an upload of the same block after the restore adds no sample | Pass | `tests/backup_restore.rs::a_restored_deployment_keeps_a_tsdb_import_and_does_not_import_it_again` | In-process broker and query equivalence |

## Test Inventory

The crate has 6 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-integration --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | A trace ID on a log resolves the trace | 1 | 1 | Complete |
| Owned surface | A deployment cut restores into an empty deployment | 3 | 3 | Complete |
| Delegated or external surface | No live Grafana container is part of these focused cross-signal tests. | — | — | Delegated or excluded |
| Delegated or external surface | A Kubernetes restore of the whole stack. | — | — | Delegated or excluded |
| **Total owned** |  | **4** | **4** | **100%** |

## Line Coverage

Run the scoped measurement with:

```bash
cargo llvm-cov nextest --package krabka-integration --profile ci --tests --lcov --output-path lcov.info
lcov --summary lcov.info
```

| File | Covered | Total | Coverage | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Scoped crate |  |  |  | Fill from the command above; CI also uploads workspace LCOV to Codecov |

The scoped command excludes ignored container suites.

## Mutation Coverage

This crate has no mutation target.

## Test Infrastructure

Tests use `assert2`, in-memory trait implementations, and checked-in fixtures where those seams apply.

Property and corpus harnesses run as ordinary Bazel and Cargo tests, and live upstream comparisons carry the `docker` tag.

## Key Gaps

| Area | Gap | Severity | Notes |
| :--- | :--- | :--- | :--- |
| Coverage measurement | No checked-in scoped line percentage | Low | Generate it with the command above |
| Compatibility boundary | No live Grafana container is part of these focused cross-signal tests. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 4 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
