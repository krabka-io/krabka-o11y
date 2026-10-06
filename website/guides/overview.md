# krabka-o11y

krabka-o11y serves metrics, logs, traces, and profiles through the APIs and query languages that Grafana already uses.

The stack uses Rust, a broker-backed write-ahead log, and a shared columnar block store over object storage.

## Evaluate the stack

Start with the [observability lab](/krabka-o11y/lab/). Then [run the stack locally](../../docs/getting_started.md) and [add Grafana datasources](../../docs/grafana.md).

| Signal | Upstream contract | Query surface |
| --- | --- | --- |
| Metrics | Prometheus and Grafana Mimir | PromQL and the Prometheus HTTP API |
| Logs | Grafana Loki | LogQL and the Loki HTTP API |
| Traces | Grafana Tempo | TraceQL, search, and trace-by-ID |
| Profiles | Grafana Pyroscope | pprof, flame graphs, and the Connect API |

## The path to 1.0

The planned 1.0 release follows full compatibility with the Mimir, Loki, Tempo, and Pyroscope contracts in scope.

The [compatibility matrix](../../docs/api_compatibility.md) distinguishes supported cases, local implementations, known gaps, and exclusions. It links each claim to executable evidence. The site does not assign compatibility to cases outside that evidence.

## Read the evidence

- [Architecture](../../docs/architecture_design.md): ingest, the write-ahead log, blocks, query roles, and tenant boundaries.
- [Deployment](../../deploy/README.md): Docker Compose, Kubernetes, bootstrap topics, and lifecycle settings.
- [Verification](../../docs/verification.md): the exact Creusot contracts and bounded Stateright models.
- [Measured performance](../../docs/operating_envelope.md): the fixed deployment, datasets, measurements, and limits.
- [Known issues](../../KNOWN_ISSUES.md): current limitations.

The [documentation directory](/krabka-o11y/docs/directory/) includes all guides, crate documentation, release reports, and contributor references.
