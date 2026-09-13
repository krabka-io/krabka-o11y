# krabka-metrics-service

PromQL query, query-frontend, and ruler service for Krabka metrics.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Quick Start

Start the metrics distributor and block builder first, then run:

```bash
bazel run //crates/metrics-service:krabka-metrics-service -- --help
bazel run //crates/metrics-service:krabka-metrics-service -- \
  --target=querier --listen=127.0.0.1:9090 \
  --object-store-url=file://./.krabka-metrics-blocks \
  --wal-bootstrap=127.0.0.1:9092
```

Send `X-Scope-OrgID` on every query.

## Configuration

The complete list is in `--help` and [`deploy/roles/`](../../deploy/roles).

| Option | Environment | Default | Description |
| --- | --- | --- | --- |
| `--target` | `KRABKA_METRICS_SERVICE_TARGET` | required | Selects `querier`, `query-frontend`, or `ruler` |
| `--listen` | `KRABKA_METRICS_SERVICE_LISTEN` | `0.0.0.0:4041` | Sets the query listen address |
| `--object-store-url` | `KRABKA_METRICS_OBJECT_STORE_URL` | `file://./.krabka-metrics-blocks` | Sets the metric block store |
| `--wal-bootstrap` | `KRABKA_METRICS_WAL_BOOTSTRAP` | none | Enables recent-sample WAL reads |
| `--config.file` | `KRABKA_CONFIG_FILE` | none | Loads role configuration from YAML |

## Documentation

- [Getting started](../../docs/getting_started.md#metrics)
- [API compatibility](../../docs/api_compatibility.md#metrics)
- [Test coverage](test_coverage_report.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_metrics_service/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
