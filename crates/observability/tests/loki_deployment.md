# Loki deployment scenarios

Run against the locally built application and digest-pinned broker and `MinIO`:

```bash
bazel test --config=docker //crates/observability:loki_deployment_docker_test
```

The scenarios adapt Grafana Loki's [integration tests](https://github.com/grafana/loki/tree/784b1ec38e3e010143bc8dd70ccbb12837f3d99f/integration), surveyed at commit `784b1ec38e3e010143bc8dd70ccbb12837f3d99f`.
Krabka's distributor, block builder, and querier replace Loki's ingester, compactor, index gateway, and query frontend. Each test owns its broker, object store, Docker network, and local state.

| Upstream scenario | Krabka test |
| --- | --- |
| `TestMicroServicesIngestQuery` | JSON, gzip, and Snappy protobuf hot queries, persisted blocks, and querier restart |
| `TestCategorizedLabels` | Structured metadata and parsed labels remain separate with `categorize-labels` |
| `TestDedupMicroServicesKafka` | Identical copies deduplicate across separate persisted batches; timestamp, stream, and metadata differences remain |
| `TestMultiTenantQuery` | Single-tenant isolation and combined tenant queries before and after persistence |
| `TestPerRequestLimits` | Adapted to tenant `max_query_length`: range and label queries reject excessive ranges, exact-limit requests succeed, and another tenant stays unrestricted |
| `Test_ExploreLogsApis` | Detected fields, cardinalities, field values, label names, and label values across storage and restart |
| `TestOTLPLogsIngestQuery` | Resource, scope, and record attribute normalization survives the WAL and object store |
| `TestMicroServicesDeleteRequest` | Cancellation preserves later writes; metadata filters remove stored rows, while another tenant remains intact |
| `TestMicroServicesIngestQuery` tail subtest | A websocket reads retained WAL backlog followed by live data without another tenant's entries; range queries separately verify persistence |
| `TestSingleBinaryIngestQuery` | An empty stored answer before shutdown proves the all-in-one role must drain an accepted, in-flight log into a readable block |

Expected timestamps, lines, labels, metadata, and field cardinalities come from explicit inputs. Tests first observe WAL-backed answers, then use a querier configured to tail the provisioned, empty traces WAL. The ingestion cases also assert an empty cold answer before a builder starts. Restart checks stop the builder and cold querier before starting a fresh cold querier, so a previous process's hot head or cache cannot satisfy the check. The cold querier retains broker-backed authorization; no log records are ever written to its WAL.

These tests complement `loki_differential.rs`, which compares public APIs with a pinned Loki, and the in-process storage, deletion, retention, ruler, and tail suites. Loki-specific bloom building, scheduler rings, and legacy storage-schema migration are not mapped onto Krabka's different storage architecture.

The per-request `X-Loki-Query-Limits` header is not implemented by Krabka. The adapted limit case exercises the existing tenant override contract; it does not claim per-request-limit compatibility.
