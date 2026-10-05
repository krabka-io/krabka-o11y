# krabka-profiles Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-profiles` |
| **Signal** | profiles |
| **Upstream surface** | Pyroscope ingest, Connect query, storage, and roles |
| **Date** | 2026-10-05 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **Pyroscope ingest, Connect query, storage, and roles** | Render output matches Pyroscope after identical ingest | Pass | `tests/pyroscope_differential.rs::real_pyroscope_render_matches_krabka_after_identical_ingest` | Pinned Pyroscope image |
| **Deployed ingestion** | JSON/binary Connect push, plain/gzip pprof, speedscope, and four OTLP transports preserve every stack and value after persistence and restart | Covered | Nine cases in `tests/pyroscope_deployment.rs` | Fixed collapsed stack/value maps adapted from pinned Pyroscope tests |
| **Metadata and tenant isolation** | Types, labels, projected series, selectors, and identical profile labels remain correct across restart and tenants | Covered | Two deployment cases | Whole metadata responses and distinct expected values |
| **Hot/storage handoff** | Separate batches add once while hot and stored copies overlap | Covered | `separate_batches_merge_without_double_counting_hot_and_cold` and namespace/union/downsampling regressions | WAL provenance; independent expected sample sums |
| **Malformed requests** | Invalid pprof string references are rejected before WAL append; query methods/content/JSON/selectors/types return expected HTTP codes | Covered | Two deployment cases and shared ingest regression | Pinned upstream status codes and absence of rejected profiles |
| **Shutdown** | The all-in-one role drains accepted samples to storage before stopping | Covered | `single_binary_shutdown_drains_profiles_to_storage` | Empty storage before stop, exact fresh-process stored result afterward |

## Test Inventory

The crate has 395 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-profiles --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Render output matches Pyroscope after identical ingest | 1 | 1 | Complete |
| Delegated or external surface | Ignored Pyroscope and Grafana suites require Docker and are excluded from the scoped line run. | — | — | Delegated or excluded |
| Deployment cases | Ingest, metadata, isolation, handoff, validation, status codes, shutdown | 15 | 15 | Implemented; Docker required |

## Line Coverage

Run the scoped measurement with:

```bash
cargo llvm-cov nextest --package krabka-profiles --profile ci --lib --bins --lcov --output-path lcov.info
lcov --summary lcov.info
```

| File | Covered | Total | Coverage | Notes |
| :--- | :--- | :--- | :--- | :--- |
| Scoped crate |  |  |  | Fill from the command above; CI also uploads workspace LCOV to Codecov |

The scoped command excludes ignored container suites.

## Mutation Coverage

The manual target is `bazel test //crates/profiles:profiles_mutants`.

Its baseline is `unseeded`, so no survivor count is quoted until every shard writes a valid totals line.

## Test Infrastructure

Tests use `assert2`, in-memory trait implementations, and checked-in fixtures where those seams apply.

Property and corpus harnesses run as ordinary Bazel and Cargo tests, and live upstream comparisons and deployed-role scenarios carry the `docker` tag.

## Key Gaps

| Area | Gap | Severity | Notes |
| :--- | :--- | :--- | :--- |
| Coverage measurement | No checked-in scoped line percentage | Low | Generate it with the command above |
| Compatibility boundary | Ignored Pyroscope and Grafana suites require Docker and are excluded from the scoped line run. | Low | The owning crate or Docker suite carries the claim |

## Conclusion

The report accounts for 394 source-declared tests and fifteen deployment scenarios, alongside the existing differential suites. These scenario counts do not imply complete upstream compatibility.

Line and mutation percentages remain unquoted until their complete tool output is available.
