# krabka-traces Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-traces` |
| **Signal** | traces |
| **Upstream surface** | OTLP, Zipkin, Jaeger ingest and Tempo query APIs |
| **Date** | 2026-09-13 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **OTLP, Zipkin, Jaeger ingest and Tempo query APIs** | Trace by ID and search match Tempo | Pass | `tests/tempo_differential.rs::real_tempo_and_krabka_match_basic_by_id_and_search` | Pinned Tempo image |

## Test Inventory

The crate has 669 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-traces --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Trace by ID and search match Tempo | 1 | 1 | Complete |
| Delegated or external surface | Ignored Tempo and Grafana suites require Docker and are excluded from the scoped line run. | — | — | Delegated or excluded |
| **Total owned** |  | **1** | **1** | **100%** |

## Line Coverage

Run the scoped measurement with:

```bash
cargo llvm-cov nextest --package krabka-traces --profile ci --lib --bins --lcov --output-path lcov.info
lcov --summary lcov.info
```

| File | Covered | Total | Coverage | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Scoped crate |  |  |  | Fill from the command above; CI also uploads workspace LCOV to Codecov |

The scoped command excludes ignored container suites.

## Mutation Coverage

The scheduled target is `bazel test //crates/traces:traces_mutants`.

Its baseline is `unseeded`, so no survivor count is quoted until every shard writes a valid totals line.

## Test Infrastructure

Tests use `assert2`, in-memory trait implementations, and checked-in fixtures where those seams apply.

Property and corpus harnesses run as ordinary Bazel and Cargo tests, and live upstream comparisons carry the `docker` tag.

## Key Gaps

| Area | Gap | Severity | Notes |
| :--- | :--- | :--- | :--- |
| Coverage measurement | No checked-in scoped line percentage | Low | Generate it with the command above |
| Compatibility boundary | Ignored Tempo and Grafana suites require Docker and are excluded from the scoped line run. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 669 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
