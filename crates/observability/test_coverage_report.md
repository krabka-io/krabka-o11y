# krabka-observability Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-observability` |
| **Signal** | logs |
| **Upstream surface** | Loki ingest, LogQL query, roles, and operational APIs |
| **Date** | 2026-09-13 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **Loki ingest, LogQL query, roles, and operational APIs** | Log query and push corpus matches Loki | Pass | `tests/loki_differential.rs::loki_corpus_matches_krabka` | Pinned Loki image |

## Test Inventory

The crate has 1052 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-observability --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Log query and push corpus matches Loki | 1 | 1 | Complete |
| Delegated or external surface | Ignored Loki and Grafana suites require Docker and are excluded from the scoped line run. | — | — | Delegated or excluded |
| **Total owned** |  | **1** | **1** | **100%** |

## Line Coverage

Run the scoped measurement with:

```bash
cargo llvm-cov nextest --package krabka-observability --profile ci --lib --bins --lcov --output-path lcov.info
lcov --summary lcov.info
```

| File | Covered | Total | Coverage | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Scoped crate |  |  |  | Fill from the command above; CI also uploads workspace LCOV to Codecov |

The scoped command excludes ignored container suites.

## Mutation Coverage

The scheduled target is `bazel test //crates/observability:observability_mutants`.

Its baseline is `unseeded`, so no survivor count is quoted until every shard writes a valid totals line.

## Test Infrastructure

Tests use `assert2`, in-memory trait implementations, and checked-in fixtures where those seams apply.

Property and corpus harnesses run as ordinary Bazel and Cargo tests, and live upstream comparisons carry the `docker` tag.

## Key Gaps

| Area | Gap | Severity | Notes |
| :--- | :--- | :--- | :--- |
| Coverage measurement | No checked-in scoped line percentage | Low | Generate it with the command above |
| Compatibility boundary | Ignored Loki and Grafana suites require Docker and are excluded from the scoped line run. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 1052 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
