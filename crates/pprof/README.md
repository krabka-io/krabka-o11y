# krabka-pprof

Pprof decoding, flame-graph query, diff, series, and symbolization engine for Krabka profiles.

Part of [krabka-o11y](../../README.md), the Krabka observability stack.

## Overview

This crate turns stored profile samples into Pyroscope-compatible trees, flame graphs, diffs, series, heatmaps, and pprof output.

`krabka-profiles` supplies the HTTP and Connect services above it.

## Features

- Pprof decode and encode
- Flame-graph merge and diff
- Label selection, series aggregation, and heatmaps
- Native symbol lookup and symbol database support

## Usage

Decode a pprof body with `PprofProfile::decode`, then use `FlameEngine` with a `ProfileStore` implementation for queries.

See the generated [rustdoc](https://krabka-io.github.io/krabka-o11y/krabka_pprof/) for the current API.

## Documentation

- [API compatibility](../../docs/api_compatibility.md#profiles)
- [Test coverage](test_coverage_report.md)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
