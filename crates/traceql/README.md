# krabka-traceql

TraceQL parser and DataFusion query engine for Krabka traces.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Overview

This crate parses TraceQL selectors, structural operators, pipelines, aggregations, and metrics queries.

It lowers trace relationships to nested-set operations over a caller-provided `SpanStore`.

## Features

- TraceQL lexer, parser, AST, and planner
- Structural trace relationships and scoped attributes
- Search, aggregation, compare, and metrics operations
- Golden corpus and property tests

## Usage

```rust
let query = krabka_traceql::parse(r#"{ resource.service.name = "api" }"#)?;
# Ok::<(), krabka_traceql::TraceqlError>(())
```

## Documentation

- [API compatibility](../../docs/api_compatibility.md#traces)
- [Test coverage](test_coverage_report.md)
- [Rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_traceql/)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
