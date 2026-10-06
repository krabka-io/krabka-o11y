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

- `functions.test`: four `double_exponential_smoothing` cases requiring
  `experimental-functions`.
- `aggregators.test`: the `limitk(NaN, …)` and `limit_ratio(NaN, …)` refusals
  requiring `experimental-functions`.
- `native_histograms.test`: three `limitk` / `limit_ratio` range queries
  requiring `experimental-functions`.

Each annotation in the file states its own reason; the list above only groups
them.

The separate `upstream-3.14.0/` qualification directory contains every upstream file at commit `d7598b7141418fa35be2b5ec5d0fefb634199610`, with no Krabka divergence annotations. Its machine report records all mismatches. It does not replace the existing regression gate.

`extended_vectors.test` was refreshed to Prometheus 3.14.0 commit
`d7598b7141418fa35be2b5ec5d0fefb634199610` when the anchored and smoothed
boundary semantics were implemented. Histogram annotation strings in the
curated fixtures use that revision's metric-name formatting. Invalid UTF-8
label-name cases now reject rather than carry a divergence annotation.

`name_label_dropping.test` also uses that 3.14.0 revision to qualify delayed
metric-name removal through composed expressions.

`type_and_unit.test` uses the same 3.14.0 revision for matching composed
expressions while metric-name removal is delayed.
