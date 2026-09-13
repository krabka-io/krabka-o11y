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
| `--bootstrap` | `KRABKA_TRACES_BOOTSTRAP` | none | Sets the broker address |
| `--object-store-url` | `KRABKA_TRACES_OBJECT_STORE_URL` | none | Sets the trace block store |
| `--config.file` | `KRABKA_CONFIG_FILE` | none | Loads role configuration from YAML |

## Compatibility decisions

Krabka accepts Jaeger data but does not serve the Jaeger query API.

Use the Tempo API or Grafana Tempo datasource for reads.

Krabka does not sample spans; apply sampling in an OpenTelemetry Collector before ingest.

## Documentation

- [Getting started](../../docs/getting_started.md#traces)
- [API compatibility](../../docs/api_compatibility.md#traces)
- [Test coverage](test_coverage_report.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_traces/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
