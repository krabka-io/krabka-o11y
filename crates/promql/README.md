# krabka-promql

PromQL parser, DataFusion planner, evaluator, and Prometheus query API for Krabka metrics.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Overview

This crate evaluates instant and range PromQL queries over a `MetricStore` and exposes Prometheus and Mimir HTTP routes.

Its vendored Prometheus corpus must pass in full, and live differential suites compare query results with Prometheus and Mimir.

## Features

- PromQL instant and range evaluation
- Native histograms, exemplars, metadata, rules, and cardinality APIs
- Query splitting, caching, limits, and merged hot and cold stores
- Optional `experimental-functions` support for upstream experimental functions

## Usage

```rust
let expression = krabka_promql::parse_promql("sum(rate(http_requests_total[5m]))")?;
# Ok::<(), krabka_promql::PromqlError>(())
```

## Documentation

- [API compatibility](../../docs/api_compatibility.md#metrics)
- [Test coverage](test_coverage_report.md)
- [Corpus attribution](tests/testdata/ATTRIBUTION.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_promql/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
