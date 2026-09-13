# krabka-metrics

Prometheus and Mimir compatible metrics ingest and block building for Krabka.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Overview

This crate accepts Prometheus remote write, OTLP metrics, and Krabka clocks, appends decoded records to the broker WAL, and builds metric blocks and indexes.

PromQL serving lives in `krabka-metrics-service` because the read crate depends on these data types.

## Features

- Prometheus remote write v1 and v2
- OTLP HTTP and gRPC metric ingest
- Float, native histogram, exemplar, metadata, and clock records
- WAL replay, block building, compaction, retention, and tenant limits

## Quick Start

```bash
bazel run //crates/metrics:krabka-metrics -- --help
bazel run //crates/metrics:krabka-metrics -- \
  --target=distributor --listen=127.0.0.1:4041 --bootstrap=127.0.0.1:9092
```

## Configuration

The complete list is in `--help` and [`deploy/roles/`](../../deploy/roles).

| Option | Environment | Default | Description |
| --- | --- | --- | --- |
| `--target` | `KRABKA_METRICS_TARGET` | required | Selects `distributor`, `block-builder`, or `compactor` |
| `--listen` | `KRABKA_METRICS_LISTEN` | `0.0.0.0:4041` | Sets the data listen address |
| `--bootstrap` | `KRABKA_METRICS_BOOTSTRAP` | `127.0.0.1:9092` | Sets the broker bootstrap address |
| `--object-store-url` | `KRABKA_METRICS_OBJECT_STORE_URL` | `file://./.krabka-metrics-blocks` | Sets the block object store |
| `--config.file` | `KRABKA_CONFIG_FILE` | none | Loads role configuration from YAML |

## Documentation

- [Getting started](../../docs/getting_started.md#metrics)
- [API compatibility](../../docs/api_compatibility.md#metrics)
- [Test coverage](test_coverage_report.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_metrics/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
