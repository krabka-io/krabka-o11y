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

## Test Inventory

The crate has 3 source-declared unit, property, corpus, or integration tests.

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
| Delegated or external surface | No live Grafana container is part of these focused cross-signal tests. | — | — | Delegated or excluded |
| **Total owned** |  | **1** | **1** | **100%** |

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

This crate has no scheduled mutation target.

## Test Infrastructure

Tests use `assert2`, in-memory trait implementations, and checked-in fixtures where those seams apply.

Property and corpus harnesses run as ordinary Bazel and Cargo tests, and live upstream comparisons carry the `docker` tag.

## Key Gaps

| Area | Gap | Severity | Notes |
| :--- | :--- | :--- | :--- |
| Coverage measurement | No checked-in scoped line percentage | Low | Generate it with the command above |
| Compatibility boundary | No live Grafana container is part of these focused cross-signal tests. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 3 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
