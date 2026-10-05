# krabka-observability Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-observability` |
| **Signal** | logs |
| **Upstream surface** | Loki ingest, LogQL query, roles, and operational APIs |
| **Date** | 2026-10-05 |

## Compatibility Coverage Summary

The owned behavior below has executable coverage, and delegated compatibility is stated as a gap.

| Surface | Behavior | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **Loki ingest, LogQL query, roles, and operational APIs** | Log query and push corpus matches Loki | Pass | `tests/loki_differential.rs::loki_corpus_matches_krabka` | Pinned Loki image |

## Test Inventory

The crate has 1126 source-declared unit, property, corpus, or integration tests.

Run the complete non-container inventory with:

```bash
cargo test --package krabka-observability --locked -- --list
```

Tests under `tests/` cover public crate boundaries, while tests under `src/` cover local behavior.

Docker-tagged differential suites are listed in the root [compatibility matrix](../../docs/api_compatibility.md).

The twelve ignored deployment cases in `tests/loki_deployment.rs` run locally built roles with a real broker and `MinIO`. Their [upstream scenario mapping](tests/loki_deployment.md) records the source revision and adaptations. Expected results come from explicit inputs; these cases establish deployed behavior without a live Loki comparison.

| Test function in `tests/loki_deployment.rs` | Scope |
| --- | --- |
| `json_push_survives_storage_and_restart` | JSON push, hot answer, empty-store control, stored answer, restart |
| `gzip_push_survives_storage_and_restart` | Gzip JSON over the same deployed path |
| `snappy_protobuf_push_survives_storage_and_restart` | Snappy protobuf over the same deployed path |
| `categorized_metadata_and_parser_labels_survive_storage` | Metadata and parsed labels remain separate |
| `duplicate_identity_includes_timestamp_stream_and_metadata` | Duplicates across persisted batches and distinct-entry controls |
| `multi_tenant_queries_preserve_isolation_across_restart` | Combined queries and tenant isolation |
| `tenant_query_limits_apply_before_and_after_storage` | Rejected ranges, exact-limit acceptance, unrestricted tenant |
| `explore_detected_fields_and_label_index_survive_storage` | Fields, cardinalities, field values, label names and values |
| `otlp_normalization_and_metadata_survive_storage` | Resource, scope, and record attribute normalization |
| `single_binary_shutdown_drains_logs_to_storage` | Accepted logs persist on graceful shutdown |
| `tail_reads_backlog_then_live_wal_without_cross_tenant_entries` | Retained WAL backlog and new entries over a websocket, followed by persisted range queries |
| `deletes_cancel_and_remove_matching_metadata_from_stored_blocks` | Cancellation, tenant isolation, physical deletion, fresh-state restart |

`tests/compactor.rs::compactor_runtime_deletes_existing_shard_rows_without_a_catalog` also checks physical deletion without Docker.

Run the deployment cases with:

```bash
bazel test --config=docker //crates/observability:loki_deployment_docker_test
```

## Coverage vs Scope

| Area | Scenario | Planned | Implemented | Status |
| :--- | :--- | :--- | :--- | :--- |
| Owned surface | Log query and push corpus matches Loki | 1 | 1 | Complete |
| Owned surface | Deployed role lifecycle with explicit input oracles | 12 | 12 | Complete |
| Known gap | Per-request `X-Loki-Query-Limits` header | — | — | Tenant overrides covered; header unsupported |
| Delegated or external surface | Ignored Loki and Grafana suites require Docker and are excluded from the scoped line run. | — | — | Delegated or excluded |
| **Total owned** |  | **13** | **13** | **100%** |

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

The manual target is `bazel test //crates/observability:observability_mutants`.

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

The report accounts for 1126 source-declared tests and one representative owned behavior at 100% scope coverage.

Line and mutation percentages remain unquoted until their complete tool output is available.
