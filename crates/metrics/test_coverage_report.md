# krabka-metrics Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-metrics` |
| **Signal** | metrics |
| **Upstream surface** | Remote write, OTLP, WAL records, blocks, and indexes |
| **Date** | 2026-10-05 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **Remote write, OTLP, WAL records, blocks, and indexes** | Remote write reaches a metric block | Pass | `tests/ingest_roundtrip.rs::remote_write_v1_lands_as_block` | End-to-end crate test |
| **Metric-family metadata** | Classic histogram metadata is indexed by the base family while samples retain their suffixed series identity | Pass | `src/compactor.rs::tests::classic_histogram_metadata_is_indexed_by_its_metric_family` | Independent bucket and family labels through WAL generation and compaction |

## Test Inventory

The crate has 437 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-metrics --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Remote write reaches a metric block | 1 | 1 | Complete |
| Owned surface | Classic histogram metadata is indexed by the base family while samples retain their suffixed series identity | 1 | 1 | Complete |
| Delegated or external surface | PromQL response behavior is delegated to krabka-promql and krabka-metrics-service. | — | — | Delegated or excluded |
| **Total owned** |  | **2** | **2** | **100%** |

## Line Coverage

Run the scoped measurement with:

```bash
cargo llvm-cov nextest --package krabka-metrics --profile ci --lib --bins --lcov --output-path lcov.info
lcov --summary lcov.info
```

| File | Covered | Total | Coverage | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Scoped crate |  |  |  | Fill from the command above; CI also uploads workspace LCOV to Codecov |

The scoped command excludes ignored container suites.

## Mutation Coverage

The manual target is `bazel test //crates/metrics:metrics_mutants`.

Its reviewed baseline is 174 survivors from a complete 24-shard run. The exact
commit, command, runner, duration, and artifact checksums are recorded in
[`qualification/milestone-19-mutation-baselines.json`](../../qualification/milestone-19-mutation-baselines.json).

## Test Infrastructure

Tests use `assert2`, in-memory trait implementations, and checked-in fixtures where those seams apply.

Property and corpus harnesses run as ordinary Bazel and Cargo tests, and live upstream comparisons carry the `docker` tag.

## Key Gaps

| Area | Gap | Severity | Notes |
| :--- | :--- | :--- | :--- |
| Coverage measurement | No checked-in scoped line percentage | Low | Generate it with the command above |
| Compatibility boundary | PromQL response behavior is delegated to krabka-promql and krabka-metrics-service. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 382 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
