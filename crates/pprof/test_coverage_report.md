# krabka-pprof Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-pprof` |
| **Signal** | profiles |
| **Upstream surface** | Pprof decode, flame graphs, diffs, series, and symbolization |
| **Date** | 2026-09-13 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **Pprof decode, flame graphs, diffs, series, and symbolization** | Golden profile merge preserves the expected tree | Pass | `tests/golden_merge.rs::full_merge_pins_four_int_levels_and_fold_before_symbolize` | Hand-written golden fixture |

## Test Inventory

The crate has 123 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-pprof --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Golden profile merge preserves the expected tree | 1 | 1 | Complete |
| Delegated or external surface | HTTP and Connect compatibility are delegated to krabka-profiles. | — | — | Delegated or excluded |
| **Total owned** |  | **1** | **1** | **100%** |

## Line Coverage

Run the scoped measurement with:

```bash
cargo llvm-cov nextest --package krabka-pprof --profile ci --lib --bins --lcov --output-path lcov.info
lcov --summary lcov.info
```

| File | Covered | Total | Coverage | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Scoped crate |  |  |  | Fill from the command above; CI also uploads workspace LCOV to Codecov |

The scoped command excludes ignored container suites.

## Mutation Coverage

The scheduled target is `bazel test //crates/pprof:pprof_mutants`.

Its reviewed baseline is 86 survivors from a complete 12-shard run. The exact
commit, command, runner, duration, and artifact checksums are recorded in
[`qualification/milestone-19-mutation-baselines.json`](../../qualification/milestone-19-mutation-baselines.json).

## Test Infrastructure

Tests use `assert2`, in-memory trait implementations, and checked-in fixtures where those seams apply.

Property and corpus harnesses run as ordinary Bazel and Cargo tests, and live upstream comparisons carry the `docker` tag.

## Key Gaps

| Area | Gap | Severity | Notes |
| :--- | :--- | :--- | :--- |
| Coverage measurement | No checked-in scoped line percentage | Low | Generate it with the command above |
| Compatibility boundary | HTTP and Connect compatibility are delegated to krabka-profiles. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 123 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
