# Pyroscope deployment scenarios

This suite adapts public API cases from [Grafana Pyroscope's integration tests](https://github.com/grafana/pyroscope/tree/3220318a9ee0a874359763d287d28a868f9fb675/pkg/test/integration).
The source pin is `3220318a9ee0a874359763d287d28a868f9fb675`.

Run the suite with Docker and the Bazel-loaded images:

```bash
bazel test --config=docker //crates/profiles:pyroscope_deployment_docker_test
```

The suite has fifteen cases. Each case owns its network, broker, MinIO, and local state.
The application roles run from the local Krabka image. The shared container helper records the loaded image's immutable content ID.

| Upstream source | Krabka deployment coverage |
| :--- | :--- |
| `ingest_pprof_test.go::TestPush` | JSON and binary Connect pushes of gzip pprof retain every stack and value after storage and restart |
| `ingest_pprof_test.go::TestIngest` | Plain and gzip pprof uploads through `/ingest` retain every stack and value |
| `ingest_speedscope_test.go::TestIngestSpeedscope` | A sampled speedscope upload retains the two stacks and their CPU values |
| `ingest_otlp_test.go::TestIngestOTLPHTTPBinary` and `TestIngestOTLPHTTPJSON` | Protobuf, JSON, and gzip protobuf exports retain exact stacks through persistence |
| `ingest_otlp_test.go::TestIngestOTLP` | The public export RPC accepts binary Connect; this case does not claim gRPC transport coverage |
| `microservices_test.go::TestMicroServicesIntegrationV1` and `TestMicroServicesIntegrationV2` | Profile types with and without a time range, label names, label values, projected series, and exact merged profiles work across deployed roles |
| `microservices_test.go` tenant query cases | Two tenants store different values under identical labels, timestamp, and profile ID; a third tenant sees no profiles or service values |
| `ingest_pprof_test.go::TestPush` and microservices merged-profile queries | Separate batches sum to fixed expected values; overlapping hot and cold tiers count each sample once |
| `ingest_pprof_test.go::TestPushStringTableOOBSampleType` | Invalid sample-type, function, mapping, and sample-label string references, plus a nonempty string-table entry zero, return HTTP 400 before WAL append |
| `http_status_codes_test.go::TestStatusCodes` | A populated deployment returns the expected codes for valid JSON, invalid JSON, unsupported content type, wrong method, malformed selector, and invalid profile type |
| `microservices_test.go::TestMicroServicesIntegrationV1/DegradedCluster` | Queries remain correct after the writer and querier stop. An additional all-in-one case checks that shutdown drains an accepted profile to storage |

The fixed pprof input has stacks `main.hotloop;main.work` and `main.work`, with values 100 and 40.
The stack order is leaf first. Expected results are fixed values, independent of the production decoder and query engine.
The suite follows upstream's `StackCollapseProto` comparison: it resolves every returned sample's stack and compares all stack values.
It also checks sample types. It does not compare protobuf ID assignments or every export metadata field.

Persistence checks first observe an empty stored answer, then start the block builder.
They stop the builder and querier and query a fresh process against the same object store.
Krabka's profiles read roles always start a WAL tail. Storage checks attach that tail to the fixture's provisioned, empty metrics WAL topic.
No metrics writer runs in the fixture, so those read roles cannot replay the ingested profiles. Each process uses a distinct consumer group to avoid waiting for a stopped member lease during the storage restart check.
The all-in-one case also checks that storage is empty before shutdown.

This suite complements `pyroscope_differential.rs`, which uses a pinned live Pyroscope as its oracle.
These deployment cases use fixed expected results and do not claim a live differential comparison.
The existing differential suite covers JFR, other legacy stack formats, additional profile types, and Grafana.

This adaptation does not reproduce Pyroscope's complete fixture corpus, V1/V2 storage internals, rings, metastore joins, replication, or federated queries.
It does not add deployment coverage for native symbolization, asynchronous querier polling, or every status-code case.
Those upstream scenarios remain outside this suite's coverage.

The deployment cases exposed three defects fixed alongside the suite: malformed pprof string references reached the WAL, Connect query GET requests returned 400 instead of 405, and stored copies were counted again while still in the hot tier.
Shared ingest validates string references before WAL append. Query RPCs accept POST only. Each WAL-backed sample carries its partition, record offset, and sample ordinal through the hot store and Parquet blocks; the union excludes a hot copy when its identity is present in storage. Separate identical records remain separate. Downsampling retains all contributing identities. Parquet blocks use format version 2 and reject version 1.
