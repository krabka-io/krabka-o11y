# krabka-observability

Loki-compatible log ingest, storage, and query roles for Krabka.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Quick Start

Run all log roles on one listener:

```bash
bazel run //crates/observability:krabka-observability -- --help
bazel run //crates/observability:krabka-observability -- \
  --target=all --listen-addr=127.0.0.1:3100 \
  --wal-bootstrap-server=127.0.0.1:9092 --object-store-url=memory:///
```

Send `X-Scope-OrgID` on every push and query.

## Configuration

The complete list is in `--help` and [`deploy/roles/`](../../deploy/roles).

| Option | Environment | Default | Description |
| --- | --- | --- | --- |
| `--target` | `KRABKA_OBSERVABILITY_TARGET` | required | Selects `distributor`, `block-builder`, `querier`, or `all` |
| `--listen-addr` | `KRABKA_OBSERVABILITY_LISTEN_ADDR` | `0.0.0.0:3100` | Sets the Loki data address |
| `--wal-bootstrap-server` | `KRABKA_OBSERVABILITY_WAL_BOOTSTRAP_SERVER` | none | Sets the broker address |
| `--object-store-url` | `KRABKA_OBSERVABILITY_OBJECT_STORE_URL` | none | Sets the log block store |
| `--config.file` | `KRABKA_CONFIG_FILE` | none | Loads role configuration from YAML |

## Documentation

- [Getting started](../../docs/getting_started.md#logs)
- [API compatibility](../../docs/api_compatibility.md#logs)
- [Test coverage](test_coverage_report.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_observability/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
