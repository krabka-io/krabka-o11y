# krabka-logql

LogQL parser, pipeline evaluator, and planner front end for Krabka logs.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Overview

This crate parses Loki stream selectors, line filters, parser stages, field filters, templates, range aggregations, vector aggregations, and metric expressions.

`krabka-observability` uses its plans to answer the Loki query API.

## Features

- LogQL stream and metric parsing
- JSON, logfmt, pattern, and regular-expression stages
- Field, line, IP, duration, and byte filters
- Property-tested parser and planner behavior

## Usage

```rust
let query = krabka_logql::parse_query(r#"{service="api"} |= "error""#)?;
# Ok::<(), krabka_logql::ParseError>(())
```

## Documentation

- [API compatibility](../../docs/api_compatibility.md#logs)
- [Test coverage](test_coverage_report.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_logql/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
