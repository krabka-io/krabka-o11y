# Vendored Prometheus PromQL conformance tests

These `.test` files are copied from:

- Upstream: https://github.com/prometheus/prometheus
- Path: `promql/promqltest/testdata/*.test`
- Pinned tag: `v3.8.1` (commit `ed753444ffec98097399d0cfa9073c70a840b812`)

Prometheus is licensed under the Apache License 2.0. The full license text is in
the upstream `LICENSE` file. These files retain their original copyright; they
are used here with trailing whitespace stripped.

`functions.test` was previously vendored, in part, at `v3.5.0`. It moved to
`v3.8.1` with the rest: the `v3.5.0` file still writes annotation expectations
as `eval_warn` / `eval_info` headers, which the harness does not read, and
`v3.8.1` writes them as `expect warn` / `expect info` directives, which it does.

`ranges.test` is NOT from Prometheus. Upstream has no file of that name at any
tag; this one is Krabka's own, and earlier revisions of this file wrongly
attributed it to `v3.5.0`. `staleness.test` was in the same position and has
since been replaced with the genuine upstream file.

## Coverage

Every upstream file is vendored WHOLE, with one exception: `limit.test` runs
only under the crate's `experimental-functions` feature, because `limitk` and
`limit_ratio` do not exist in a default build.

## Known divergences

A case the engine is known to get wrong carries a `# krabka:divergence <reason>`
comment on the line above it. Prometheus reads that line as a comment, so the
file stays a valid upstream `.test` file; the harness reads it as an annotation
and REQUIRES the case to fail, so a divergence that is later fixed cannot go on
being annotated as one. The conformance report prints every annotated divergence
under its file.

`# krabka:divergence-without-experimental-functions <reason>` is the same thing
for a case that needs an experimental function: it diverges in a default build
and is an ordinary case under `experimental-functions`.

The divergences, by file:

- `functions.test` (5 of 370): four `double_exponential_smoothing` cases, which
  need `experimental-functions`; and `label_replace(testmetric, "\xff", …)`,
  whose destination label name is an invalid UTF-8 byte that a Rust `String`
  cannot carry, so the label-name validation that upstream fails on never sees
  it.
- `aggregators.test` (3 of 160): `count_values("a\xc5z", …)`, an invalid UTF-8
  label name; and the `limitk(NaN, …)` and `limit_ratio(NaN, …)` refusals,
  which need `experimental-functions`.
- `native_histograms.test` (3 of 374): the three `limitk` / `limit_ratio` range
  queries, which need `experimental-functions`.

Each annotation in the file states its own reason; the list above only groups
them.
