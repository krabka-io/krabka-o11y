# Full Grafana compatibility roadmap

This roadmap defines full client compatibility with Grafana Mimir, Loki, Tempo, and Pyroscope.
It turns the remaining work into ordered packages with executable completion gates.

## Result

Krabka has broad support for all four products, but the current evidence does not prove full compatibility.
The product epics remain open even though all their child issues are closed.
Milestones 8 and 9 also remain open with no open issues.
([metrics epic](https://github.com/krabka-io/krabka-o11y/issues/7), [logs epic](https://github.com/krabka-io/krabka-o11y/issues/8), [traces epic](https://github.com/krabka-io/krabka-o11y/issues/9), [profiles epic](https://github.com/krabka-io/krabka-o11y/issues/10))

The first new milestone must freeze the compatibility contract and update every oracle.
Product work can then run in parallel.
One final milestone must qualify the complete stack from an immutable commit.

## Compatibility boundary

Full compatibility means that an existing supported client does not need a Krabka-specific code path.
The contract includes these surfaces:

- The data plane includes public ingest, query, streaming, and metadata APIs.
- The tenant control plane includes limits, rules, alerts, deletion, and configuration APIs.
- The operator plane includes public health, status, lifecycle, and configuration APIs.
- Grafana, Alloy, and the official product clients must complete their supported workflows without an adapter.

Internal service protocols are not part of this contract.
Ring and memberlist APIs need a real Krabka equivalent or an explicit topology-specific exclusion.
Go runtime profiles and undocumented debug APIs are also excluded.
The [API compatibility matrix](../api_compatibility.md) must name every exclusion.

Documentation is not the behavior oracle.
Each compatibility claim needs a live comparison with an immutable upstream image.

## Target baseline

| Product   | Current oracle                                      | First target                                                       | Main evidence gap                                                                                               |
| --------- | --------------------------------------------------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| Mimir     | 2.16.1                                              | [3.2.1](https://github.com/grafana/mimir/releases/tag/mimir-3.2.1) | The live test seeds remote-write v1 and compares successful instant and range queries only.                     |
| Loki      | 3.5.1                                               | [3.7.7](https://github.com/grafana/loki/releases/tag/v3.7.7)       | The suite is broad, but it has seven recorded divergences and incomplete OTLP, limits, and lifecycle coverage.  |
| Tempo     | A mutable `latest` label                            | [3.0.3](https://github.com/grafana/tempo/releases/tag/v3.0.3)      | The route set and live TraceQL corpus do not cover current streaming, override, and metrics-generator behavior. |
| Pyroscope | A mutable `latest` label whose digest reports 2.2.1 | [2.3.1](https://github.com/grafana/pyroscope/releases/tag/v2.3.1)  | The local public protocol is a subset, and Grafana and ingest coverage target older clients.                    |

The repository stores image names in [`bazel/images/images.bzl`](../../bazel/images/images.bzl) and digests in [`MODULE.bazel`](../../MODULE.bazel).
The two files must identify the same explicit release for each product.

## Current evidence

| Product   | Evidence that exists now                                                                                                                                                        | Assessment                                                                                                              |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Mimir     | [`diff_mimir`](../../crates/metrics-service/tests/diff_mimir.rs), the Prometheus corpora, remote-write v1 and v2, OTLP, a ruler, and Grafana integration                        | The Prometheus core is strong. Most Mimir-specific APIs and failure semantics have no live comparison.                  |
| Loki      | [`loki_differential`](../../crates/observability/tests/loki_differential.rs), LogQL, native and OTLP ingest, a ruler, deletion, retention, and Grafana end-to-end tests         | The suite covers many routes. Current Loki behavior, advanced OTLP mapping, and durable lifecycle behavior remain open. |
| Tempo     | [`tempo_differential`](../../crates/traces/tests/tempo_differential.rs), a vendored TraceQL corpus, current receiver families, metrics generation, and Grafana end-to-end tests | The core data path exists. The current API, full TraceQL corpus, and metrics-generator options remain open.             |
| Pyroscope | [`pyroscope_differential`](../../crates/profiles/tests/pyroscope_differential.rs), pprof and OTLP ingest, public query RPCs, symbolization, blocks, and Grafana integration     | The profile core is broad. The current protocol, upload services, and Profiles Drilldown workflows remain open.         |

The generated [route inventory](../api/routes.json) records 220 Krabka routes.
It does not classify those routes against a tagged upstream source.

## Delivery order

| Phase | Output                                              | Depends on                       | Can run with |
| ----- | --------------------------------------------------- | -------------------------------- | ------------ |
| C0    | Shared compatibility contract and immutable oracles | None                             | None         |
| M     | Mimir 3.2.1 compatibility                           | C0                               | L, T, P      |
| L     | Loki 3.7.7 compatibility                            | C0                               | M, T, P      |
| T     | Tempo 3.0.3 compatibility                           | C0                               | M, L, P      |
| P     | Pyroscope 2.3.1 compatibility                       | C0                               | M, L, T      |
| Q     | Cross-product qualification                         | Required M, L, T, and P packages | None         |

Each package below is one reviewable issue or one small issue group.
The implementation plan should batch packages whose file sets do not overlap.

## C0: Lock the contract and the oracles

1. Pin each target tag and image digest.
2. Record the image-reported version and upstream Git commit.
3. Generate tagged HTTP route and public protocol manifests for all four products.
4. Classify each upstream surface as `required`, `role-equivalent`, or `excluded`.
5. Link every `supported` matrix row to one executable differential case.
6. Add a reviewed process for later upstream version changes.
7. Close milestones 8 and 9 after their merged work has final evidence.
8. Keep all new compatibility work in new milestones.

Exit gate:

- A manifest check fails when a tagged upstream adds an unclassified public surface.
- A version smoke test starts each digest-pinned image and checks its reported version.
- The compatibility matrix has no unsupported claim and no unexplained divergence.

C0 blocks every product package.

## M: Grafana Mimir 3.2.1

### M1: Complete ingest compatibility

Compare remote-write v1 and v2 for floats, histograms, custom buckets, exemplars, metadata, stale markers, and created timestamps.
Cover duplicate labels, ordering, validation, HA deduplication, limits, out-of-order writes, and partial rejection.
Compare OTLP protobuf, gzip, delta state, translation headers, tenant metadata, and partial success.
Add the documented Influx line-protocol endpoint.

Gate: Equal requests produce equal status, required headers, stored series, metadata, and stable error classes on both systems.

### M2: Close PromQL and query API gaps

Remove all Krabka-owned divergences from the vendored PromQL corpus.
Add Mimir metric-name, label-name, label-value search, and native-histogram cardinality APIs.
Encode native histograms in streamed remote-read responses.
Compare query parameters, warnings, infos, statistics, cancellation, limits, cache headers, and partial responses.
Run range queries through direct, split, sharded, cached, retried, and partial-failure paths.

Gate: Prometheus and Mimir differential suites pass with no Krabka-owned divergence entry.

### M3: Match the ruler and Alertmanager contracts

Compare rule CRUD, validation, pagination, tenant operator routes, evaluation results, templates, and restart recovery.
Compare recording series and Alertmanager v2 payloads after restart and replica failover.
Add the multitenant Alertmanager configuration and client APIs under the Mimir prefixes.

Gate: The same rules produce equal series, alert state, errors, and notifications on both systems.

### M4: Match tenant data management and role operations

Add live tenant limits, ingestion statistics, and safe runtime-configuration output.
Add block upload and tenant deletion with state, retry, restart, and completion behavior.
Map shutdown and downscale actions to real Krabka role state.
Publish explicit exclusions for internal gRPC, ring editing, and Go profiling.

Gate: Every Mimir 3.2.1 public route has a passing test, a role-equivalent test, or an explicit exclusion.

M1 and M2 can run in parallel.
M3 depends on M1 and M2.
M4 depends on M1.

Primary sources: [HTTP API](https://github.com/grafana/mimir/blob/mimir-3.2.1/docs/sources/mimir/references/http-api/_index.md), [route registration](https://github.com/grafana/mimir/blob/mimir-3.2.1/pkg/api/api.go), [3.2 release notes](https://github.com/grafana/mimir/blob/mimir-3.2.1/docs/sources/mimir/release-notes/v3.2.md).

## L: Grafana Loki 3.7.7

### L1: Match OTLP mapping and ingest validation

Match Loki's default resource-label allowlist.
Keep other resource attributes, scope data, and log attributes as structured metadata.
Add tenant-specific attribute actions, nested-value flattening, zero-timestamp fallback, metadata caps, and line truncation.
Compare partial acceptance and discard accounting.

Gate: HTTP and protobuf corpora produce equal labels, metadata, bodies, timestamps, responses, and discard metrics.

### L2: Close stable LogQL differences

Resolve the current selector, selected-JSON, pattern parser, label collision, and malformed-query differences.
Vendor or adapt the tagged upstream syntax and engine cases.
Cover all aggregations, binary modifiers, parsers, stages, templates, offsets, special floats, and duplicate timestamps.
Keep Krabka-only syntax outside the Loki compatibility claim.

Gate: The syntax and result corpora have no unrecorded difference against Loki 3.7.7.

### L3: Complete the public HTTP and client surface

Generate the route and method set from the tagged Loki source.
Cover tail and index POST methods, tenant limits, drilldown limits, cache generation, and stable aliases.
Compare `since`, `interval`, direction, repeated matchers, volume controls, cache bypass, response headers, and error negotiation.

Gate: Grafana, `logcli`, and Alloy work unchanged against secured and unsecured deployments.

### L4: Match ruler, deletion, limits, and durable lifecycle behavior

Compare scheduled rules, recording writes, Alertmanager delivery, retries, state recovery, and rule error isolation.
Add stream-selector retention and the stable ingest, query, cardinality, tail, and metadata limits.
Compare overlapping deletion, cancellation, cache invalidation, compaction, restart, and missing-block behavior.
Match the opt-in rule for federated tenant queries.

Gate: A hot-to-cold lifecycle returns equal queries and rule state before and after retention, deletion, and restart.

L1 and L2 can run in parallel.
L3 depends on L1 and L2.
L4 depends on L1.

Primary sources: [HTTP API](https://grafana.com/docs/loki/latest/reference/loki-http-api/), [tagged route registration](https://github.com/grafana/loki/blob/v3.7.7/pkg/loki/modules.go), [LogQL reference](https://grafana.com/docs/loki/latest/query/query_reference/), [OTLP mapping](https://grafana.com/docs/loki/latest/send-data/otel/), [3.7 release notes](https://grafana.com/docs/loki/latest/release-notes/v3-7/).

## T: Grafana Tempo 3.0.3

### T1: Complete the external API

Add the public `StreamingQuerier` gRPC contract and HTTP streaming path.
Match all versioned methods for user-configurable overrides.
Compare query parameters, errors, content negotiation, JSON, protobuf, and streaming behavior.
Classify the optional MCP API separately and implement it only if it is in the supported boundary.

Gate: The route and protocol differential passes in all-in-one and frontend-to-querier deployments.

### T2: Close TraceQL conformance gaps

Import the tagged upstream parser and engine fixture corpus.
Close parser, planner, engine, metrics-query, and error differences.
Run each query against live data, cold blocks, and the query frontend.
Keep each intentional difference in a machine-readable file.

Gate: The complete pinned corpus passes, or each remaining case has an approved exclusion.

### T3: Match the metrics generator

Add host-info processing and configurable subprocessors.
Generate classic, native, or both histogram types as configured.
Match span-name sanitization, tracestate multipliers, database attributes, and instance or target information.
Enforce per-label cardinality limits and compare discard metrics.
Match remote-write tenant headers, custom headers, authentication, and retry behavior.

Gate: Seeded spans produce equal labels, samples, exemplars, histograms, and discard counters for every processor mode.

### T4: Match operations, security, and lifecycle behavior

Add safe concurrency and version checks to user-configurable overrides.
Compare cross-tenant queries, lag cutoffs, readiness, status, configuration, and public role state.
Test retention, compaction, rebalance, restart, rolling upgrade, and object-store failures.
Check tenant isolation on every HTTP and gRPC ingest and query path.

Gate: The failure suite proves no silent loss, no tenant leak, bounded recovery, and truthful readiness.

T1 through T4 can run in parallel after C0.

Primary sources: [HTTP API](https://grafana.com/docs/tempo/latest/api_docs/), [TraceQL](https://grafana.com/docs/tempo/latest/traceql/construct-traceql-queries/), [metrics generator](https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-generator/), [user-configurable overrides](https://grafana.com/docs/tempo/latest/operations/manage-advanced-systems/user-configurable-overrides/), [authentication](https://grafana.com/docs/tempo/latest/operations/authentication/).

## P: Grafana Pyroscope 2.3.1

### P1: Lock the public protocol and complete ingest

Vendor the tagged public protobuf files without local structural changes.
Generate route, service, message, and field inventories.
Add `/pyroscope/ingest` and match OTLP JSON, protobuf content types, gzip, body limits, and failures.
Compare JFR, speedscope, pprof, time parsing, aggregation, malformed bodies, and all emitted series.

Gate: Equal bytes and headers produce equal status, labels, profile types, and profile output for every supported ingest path.

### P2: Match the public query service

Add the current typed stack, trace, and span selectors.
Add pprof output, async query fields, exemplar trace IDs, and optional-field semantics.
Match UTF-8 label opt-in behavior and keep deprecated public RPCs working.
Compare all aggregation modes, grouping, limits, heatmaps, exemplars, annotations, statistics, diffs, and Connect errors.
Replace placeholder `AnalyzeQuery` counts with real values.

Gate: JSON and binary Connect cases cover every public RPC, and each selector proves that it excludes data.

### P3: Add client-compatible debug information and symbolization

Implement the public debug-information service and upload path.
Store tenant-scoped state with caps and safe build-ID validation.
Connect the existing offline symbolizer to that store.
Expose useful retry, timeout, and cache metrics.

Gate: `profilecli` uploads an ELF, state survives restart, and a later pass resolves the profile without a query-time fetch.

### P4: Complete Grafana and tenant workflows

Add persistent settings, ad hoc profile upload and diff, and recording-rule CRUD with evaluation.
Support current Profiles Drilldown and datasource protocol fields.
Classify VCS features as supported or optional, and expose truthful capability data.
Test tenant limits, deletion, status, readiness, configuration, audit, retention, compaction, restart, and rebalance.

Gate: A real Grafana session completes query, diff, heatmap, exemplar, upload, settings, and recording-rule workflows after restart.

P1 and P2 can run in parallel.
P3 can run with both.
P4 depends on P2 and P3.

Primary sources: [server API](https://github.com/grafana/pyroscope/blob/v2.3.1/docs/sources/reference-server-api/index.md), [Querier schema](https://github.com/grafana/pyroscope/blob/v2.3.1/api/querier/v1/querier.proto), [debug-information schema](https://github.com/grafana/pyroscope/blob/v2.3.1/api/debuginfo/v1alpha1/debuginfo.proto), [route registration](https://github.com/grafana/pyroscope/blob/v2.3.1/pkg/api/api.go), [Profiles Drilldown 2.3](https://github.com/grafana/profiles-drilldown/releases/tag/v2.3.0).

## Q: Immutable cross-product qualification

Q starts after all required product packages pass.

1. Run ordinary Bazel and Cargo gates from one immutable commit.
2. Run every differential suite with the declared product and Grafana digests.
3. Run Grafana Explore, dashboards, alerting, drilldowns, exemplars, and cross-signal correlation.
4. Run official client and Alloy smoke tests without adapters.
5. Exercise hot data, cold blocks, compaction, retention, deletion, cache, restart, and rolling upgrade.
6. Exercise S3 and Kafka failure cases, backpressure, cancellation, and cardinality limits.
7. Test authentication, TLS, tenant isolation, and destructive-operation audit on every public path.
8. Publish a machine-readable report with the commit, digests, manifests, commands, case counts, and results.

Exit gate:

```text
bazel run //tools/format
bazel test //...
bazel test --config=docker //crates/...
bazel test --config=scale //crates/...
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

The exact target list can split across CI jobs.
Every required job must complete at the same commit.

## Definition of done

The stack is fully compatible with the declared boundary when all these statements are true:

- One source of truth records each upstream tag, Git commit, image name, and digest.
- The tagged public route and protocol manifests have no unclassified entry.
- Every required matrix row links to a passing live differential case.
- No suite has an unexplained or Krabka-owned expected divergence.
- Grafana, Alloy, and official clients complete the supported workflows without adapters.
- The same user-visible results survive frontend routing, compaction, retention, deletion, restart, and rolling upgrade.
- Every public path enforces the declared authentication, tenant isolation, and limit behavior.
- The final report identifies the immutable Krabka commit and all external artifact digests.
- Epics 7 through 10 close only after the final report passes.
