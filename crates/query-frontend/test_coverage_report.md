# krabka-query-frontend Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-query-frontend` |
| **Signal** | all |
| **Upstream surface** | Query planning, bounded fan-out, retries, cache, and merge orchestration |
| **Date** | 2026-09-13 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **Query planning, bounded fan-out, retries, cache, and merge orchestration** | Planned queries execute with bounded concurrency | Pass | `src/tests.rs::fan_out_is_bounded_and_preserves_plan_order` | Hand-written scheduler test |

## Test Inventory

The crate has 7 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-query-frontend --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Planned queries execute with bounded concurrency | 1 | 1 | Complete |
| Delegated or external surface | Signal-specific merge compatibility remains in each signal crate. | — | — | Delegated or excluded |
| **Total owned** |  | **1** | **1** | **100%** |

## Line Coverage

Run the scoped measurement with:

```bash
cargo llvm-cov nextest --package krabka-query-frontend --profile ci --lib --bins --lcov --output-path lcov.info
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
| Compatibility boundary | Signal-specific merge compatibility remains in each signal crate. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 7 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
