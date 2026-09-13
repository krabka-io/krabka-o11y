# krabka-traceql Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-traceql` |
| **Signal** | traces |
| **Upstream surface** | TraceQL parsing, planning, search, and metrics evaluation |
| **Date** | 2026-09-13 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **TraceQL parsing, planning, search, and metrics evaluation** | Golden TraceQL cases parse and execute | Pass | `tests/golden_corpus.rs::golden_case_file` | Vendored golden corpus |

## Test Inventory

The crate has 417 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-traceql --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Golden TraceQL cases parse and execute | 1 | 1 | Complete |
| Delegated or external surface | Live Tempo response compatibility is delegated to krabka-traces. | — | — | Delegated or excluded |
| **Total owned** |  | **1** | **1** | **100%** |

## Line Coverage

Run the scoped measurement with:

```bash
cargo llvm-cov nextest --package krabka-traceql --profile ci --lib --bins --lcov --output-path lcov.info
lcov --summary lcov.info
```

| File | Covered | Total | Coverage | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Scoped crate |  |  |  | Fill from the command above; CI also uploads workspace LCOV to Codecov |

The scoped command excludes ignored container suites.

## Mutation Coverage

The scheduled target is `bazel test //crates/traceql:traceql_mutants`.

Its baseline is `unseeded`, so no survivor count is quoted until every shard writes a valid totals line.

## Test Infrastructure

Tests use `assert2`, in-memory trait implementations, and checked-in fixtures where those seams apply.

Property and corpus harnesses run as ordinary Bazel and Cargo tests, and live upstream comparisons carry the `docker` tag.

## Key Gaps

| Area | Gap | Severity | Notes |
| :--- | :--- | :--- | :--- |
| Coverage measurement | No checked-in scoped line percentage | Low | Generate it with the command above |
| Compatibility boundary | Live Tempo response compatibility is delegated to krabka-traces. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 417 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
