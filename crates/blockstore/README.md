# krabka-blockstore

Signal-agnostic columnar block storage for Krabka observability.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Overview

This crate stores metric, log, span, and profile blocks as Parquet in an `object_store` backend.

It supplies tenant paths, indexes, compaction, retention, bounded scans, and DataFusion table integration to the four signal crates.

## Features

- Immutable Parquet blocks with sidecar indexes
- Signal-specific schemas over shared block lifecycle code
- Object-store retries, retention, erasure, and compaction helpers
- DataFusion scan tables and scan reports

## Usage

Create a `BlockStore` with an `object_store::ObjectStore` and its base URL, then pass signal-specific `ScanTableRequest` values to its scan APIs.

See the generated [`BlockStore` rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_blockstore/struct.BlockStore.html) for the current signatures.

## Documentation

- [Architecture](../../docs/architecture_design.md)
- [Test coverage](test_coverage_report.md)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
