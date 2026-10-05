# krabka-traces Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-traces` |
| **Signal** | traces |
| **Upstream surface** | OTLP, Zipkin, Jaeger ingest and Tempo query APIs |
| **Date** | 2026-10-05 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **OTLP, Zipkin, Jaeger ingest and Tempo query APIs** | Trace by ID and search match Tempo | Pass | `tests/tempo_differential.rs::real_tempo_and_krabka_match_basic_by_id_and_search` | Pinned Tempo image |
| **Tempo 3.0.3 overrides API** | Versioned replace, merge patch, and delete semantics | Pass | `limits::overrides_api_response::tests::mutations_follow_tempo_preconditions_and_etags` | Deterministic in-process contract test |
| **Tempo tag scopes** | Span-only cold queries exclude resource, event, link, and intrinsic names | Pass | `querier::store::tests::cold_span_tag_discovery_keeps_other_scopes_out` | Hand-written real-block regression; fails before the fix |
| **Tempo metrics generator** | Host-info labels and configurable span-metrics subprocessors | Pass | `metricsgen::spanmetrics::tests::host_info_and_subprocessors_follow_tempo_configuration` | Deterministic processor test |

## Test Inventory

The crate has 699 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-traces --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

The twelve ignored deployment cases in `tests/tempo_deployment.rs` run real Krabka role processes with the broker WAL and `MinIO`. The [scenario mapping](tests/tempo_deployment.md) records the pinned upstream source, expected results, and excluded scenarios.

| Test function | Scope |
| --- | --- |
| `otlp_http_survives_storage_and_restart` | Protobuf HTTP receiver, complete spans, stored blocks, and restart |
| `gzip_otlp_http_survives_storage_and_restart` | Compressed protobuf HTTP ingest through storage and restart |
| `otlp_grpc_survives_storage_and_restart` | gRPC receiver through storage and restart |
| `zipkin_receiver_survives_storage_and_restart` | Zipkin JSON receiver through storage and restart |
| `jaeger_grpc_receiver_survives_storage_and_restart` | Jaeger gRPC receiver through storage and restart |
| `trace_by_id_assembles_separate_persisted_batches_without_duplicates` | Trace assembly and duplicate suppression across separate persisted batches |
| `traceql_search_uses_structural_relationships_after_restart` | Selector, ancestor, count, and unmatched queries |
| `tag_scopes_and_typed_values_survive_storage_and_restart` | Span and resource scopes, typed V2 values, and V1 service values |
| `tag_names_with_special_characters_survive_storage_and_restart` | Encoded attribute names and values |
| `tenants_keep_the_same_trace_id_isolated_across_restart` | Trace and tag isolation with the same trace ID in two tenants |
| `otlp_ingest_limit_rejects_a_batch_without_leaking_spans` | HTTP and gRPC rejection, valid boundary, and unrestricted tenant control |
| `single_binary_shutdown_drains_an_in_flight_trace_to_storage` | All-in-one query frontend and drain into stored blocks |

Run this inventory with:

```bash
bazel test --config=docker //crates/traces:tempo_deployment_docker_test
```

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Trace by ID and search match Tempo | 1 | 1 | Complete |
| Cold tag scopes | Scope discovery from stored attributes | 1 | 1 | Complete |
| Deployment paths | Twelve Tempo-inspired ingest, query, storage, isolation, limit, and drain cases | 12 | 12 | Complete |
| Delegated or external surface | Ignored Tempo and Grafana suites require Docker and are excluded from the scoped line run. | — | — | Delegated or excluded |
| **Total owned** |  | **14** | **14** | **100%** |

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

The manual target is `bazel test //crates/traces:traces_mutants`.

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

The report accounts for 699 source-declared tests and fourteen named owned scenarios. This inventory does not establish complete API, line, or mutation coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
