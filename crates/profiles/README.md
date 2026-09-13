# krabka-profiles

Grafana Pyroscope compatible profile ingest, storage, and query roles for Krabka.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Quick Start

Run all profile roles on one listener:

```bash
bazel run //crates/profiles:krabka-profiles -- --help
bazel run //crates/profiles:krabka-profiles -- \
  --target=all --listen=127.0.0.1:4040 \
  --bootstrap=127.0.0.1:9092 --object-store-url=memory:///
```

Send `X-Scope-OrgID` on every ingest and query.

## Configuration

The complete list is in `--help` and [`deploy/roles/`](../../deploy/roles).

| Option | Environment | Default | Description |
| --- | --- | --- | --- |
| `--target` | `KRABKA_PROFILES_TARGET` | required | Selects a profile role or `all` |
| `--listen` | `KRABKA_PROFILES_LISTEN_ADDR` | `0.0.0.0:4040` | Sets the ingest or query address |
| `--bootstrap` | `KRABKA_PROFILES_BOOTSTRAP` | `127.0.0.1:9092` | Sets the broker address |
| `--object-store-url` | `KRABKA_PROFILES_OBJECT_STORE_URL` | none | Sets the profile block store |
| `--config.file` | `KRABKA_CONFIG_FILE` | none | Loads role configuration from YAML |

## Documentation

- [Getting started](../../docs/getting_started.md#profiles)
- [API compatibility](../../docs/api_compatibility.md#profiles)
- [Test coverage](test_coverage_report.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_profiles/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
