# krabka-traces

Grafana Tempo compatible trace ingest, storage, query, and metrics-generation roles for Krabka.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Quick Start

Run all trace roles in one process:

```bash
bazel run //crates/traces:krabka-traces -- --help
bazel run //crates/traces:krabka-traces -- \
  --target=all --listen=127.0.0.1:3200 \
  --bootstrap=127.0.0.1:9092 --object-store-url=memory:///
```

Send `X-Scope-OrgID` as HTTP or gRPC metadata.

## Configuration

The complete list is in `--help` and [`deploy/roles/`](../../deploy/roles).

| Option | Environment | Default | Description |
| --- | --- | --- | --- |
| `--target` | `KRABKA_TRACES_TARGET` | required | Selects a trace role or `all` |
| `--listen` | `KRABKA_TRACES_LISTEN` | `0.0.0.0:3200` | Sets the Tempo query address |
| `--grpc-listen` | `KRABKA_TRACES_GRPC_LISTEN` | `0.0.0.0:4317` | Sets OTLP gRPC ingest |
| `--otlp-http-listen` | `KRABKA_TRACES_OTLP_HTTP_LISTEN` | `0.0.0.0:4318` | Sets OTLP HTTP ingest |
| `--bootstrap` | `KRABKA_TRACES_BOOTSTRAP` | `127.0.0.1:9092` | Sets the broker address |
| `--object-store-url` | `KRABKA_TRACES_OBJECT_STORE_URL` | `memory:///` | Sets the trace block store |
| `--config.file` | `KRABKA_CONFIG_FILE` | none | Loads role configuration from YAML |

## Compatibility decisions

Krabka accepts Jaeger data but does not serve the Jaeger query API.

Use the Tempo API or Grafana Tempo datasource for reads.

Krabka does not sample spans; apply sampling in an OpenTelemetry Collector before ingest.

## Deployment tests

Run the [Tempo deployment scenarios](tests/tempo_deployment.md) against real Krabka roles, the broker WAL, and `MinIO`:

```bash
bazel test --config=docker //crates/traces:tempo_deployment_docker_test
```

## Retention performance

The live store indexes finite-retention spans by start timestamp. Advancing the
WAL frontier visits only expired spans, and removes them individually without
scanning or shifting the rest of a trace. The index holds one key per retained
span; unbounded retention does not build it. Arrival order and duplicates are
preserved for query results and whole-trace metadata.

Compare the former scan with indexed expiration:

```bash
cargo run -p krabka-traces --release --example livestore_retention_profile
```

The optional final argument sets the largest retained population (default
32,768). The driver sweeps three populations, times both initial fill and a
steady retention window, and tests many traces as well as one long trace.
It also reports trace-query latency, including lock wait, while a concurrent
writer ingests batches of 64 spans under a Tokio write lock. These are local
in-memory measurements, excluding WAL transport and decoding.

A local release run (Rust 1.98.1, x86-64 Linux) with four tenants and 256
traces measured the following. Query p95 includes read-lock wait during ingestion.

| Retention window (spans) | Scan ingest (µs/span) | Indexed ingest (µs/span) | Scan query p95 (ms) | Indexed query p95 (ms) |
| --- | --- | --- | --- | --- |
| 2,048 | 2.129 | 0.403 | 0.226 | 0.064 |
| 8,192 | 9.560 | 0.689 | 1.178 | 0.180 |
| 32,768 | 37.637 | 0.774 | 3.958 | 0.175 |

Ingest timings above are for the steady retention window. The single-trace case
at 32,768 spans measured 154.762 µs/span for scanning and 0.572 µs/span for indexed
expiration. These measurements exercise the in-memory store and its lock.

## Documentation

- [Getting started](../../docs/getting_started.md#traces)
- [API compatibility](../../docs/api_compatibility.md#traces)
- [Test coverage](test_coverage_report.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_traces/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
