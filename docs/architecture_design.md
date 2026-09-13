# Four-Signal Storage Architecture Design

Krabka serves four observability signals over one columnar storage model and the APIs that their existing clients use.

## Design Goals

The stack must accept standard telemetry protocols, isolate tenants, preserve acknowledged writes through role restarts, and answer Prometheus, Loki, Tempo, and Pyroscope query clients without a translation proxy.

The implementation must reuse one object-store and query foundation without forcing all four signals into one schema.

## Architecture Overview

Each signal has a distributor, a WAL in the broker, a block builder, immutable Parquet blocks in object storage, and a querier.

The block store owns the shared Parquet, index, object-path, tenant, and DataFusion integration.

Signal crates add metric, log, span, or profile schemas and their indexes above that layer.

```text
collectors -> distributor -> broker WAL -> block builder -> object storage
                    |                              |
                    +---------- hot read ----------+-> querier -> Grafana
```

Metrics and logs tail the WAL on the read path.

Traces use a live-store role, and profiles keep a WAL-tail store for recent data.

## Key Design Decisions

### Keep one storage engine and separate signal schemas

All signals need tenant paths, immutable blocks, index snapshots, object-store access, and bounded parallel scans.

Sharing those mechanisms reduces storage code while separate schemas retain each upstream signal's labels, timestamps, and identity rules.

The rejected alternative was four independent storage engines, which would duplicate recovery and object-store behavior.

### Use DataFusion below each query language

PromQL, LogQL, TraceQL, and profile selection have different syntax and result shapes, but each needs filtering, grouping, aggregation, and Parquet scans.

Each query crate lowers its own semantics to DataFusion plans and keeps upstream behavior at the language boundary.

A shared query language was rejected because it would make Grafana clients depend on a Krabka-specific translation.

### Put acknowledged writes in the broker first

A distributor acknowledges a write after the WAL append succeeds.

Block builders can then replay from committed offsets and publish immutable blocks before they advance progress.

Direct object-store writes on the request path were rejected because they increase request latency and make partial recovery harder to define.

### Keep collection and trace sampling outside the storage stack

Prometheus agent mode, Grafana Alloy, and the OpenTelemetry Collector already own scraping, discovery, and sampling policy.

Krabka therefore accepts pushed telemetry and stores every accepted trace span.

Rate limits reject excess requests instead of selecting an incoherent subset of spans.

### Expose upstream query APIs, not every upstream product component

Krabka implements the query surfaces used by Grafana for Prometheus, Loki, Tempo, and Pyroscope.

Jaeger remains an ingest protocol, and profile label queries use the Pyroscope Connect API rather than the legacy HTTP label routes.

This boundary avoids duplicate query APIs when the Grafana integrations already use the implemented surfaces.

## Integration

`krabka-protocol` supplies shared identifiers and units, and `krabka-client-rs` supplies broker clients.

Object storage is configured with a URL and provider environment variables.

Every data-plane request carries `X-Scope-OrgID`, and the tenant is part of WAL records, object paths, indexes, and authorization checks.

Roles can run separately or, where supported, under `--target all` with one aggregate readiness result and staged shutdown.

## Upstream Compatibility

The [API compatibility matrix](api_compatibility.md) maps each compatibility claim to a live differential suite.

The generated [route inventory](api/routes.json) records every served method and path.

PromQL and TraceQL also use vendored upstream corpora for language behavior.

## Testing

The [project coverage report](test_coverage_report.md) links each crate's report.

Container suites compare Krabka with pinned Prometheus, Mimir, Loki, Tempo, Pyroscope, and Grafana images.
