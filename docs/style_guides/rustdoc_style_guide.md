# Rustdoc Style Guide

This guide defines conventions for rustdoc comments across Krabka crates. It follows the patterns that `krabka-blockstore`, `krabka-promql`, `krabka-traceql`, and `krabka-pprof` already use. It complements the general [code style guide](code_style_guide.md).

This guide defines **structure**. The [prose style guide](prose_style_guide.md) defines **wording**, and it applies to every doc comment: Simplified Technical English, one short summary sentence on the first line, and `must` only where the code enforces the rule.

## Crate-Level Documentation

Every library crate must have a crate-level doc comment at the top of `lib.rs`. Binary crates (`main.rs`) do not need one.

Krabka uses `//!` line comments for crate-level docs, not `/*! … */` blocks. This matches the existing crates. It also avoids a limit of `/* */` blocks: they cannot contain a `*/` sequence. That limit matters here, because doc text often mentions glob patterns, regexes, and byte sequences.

The crate-level doc should include:

1. **One-line summary** — what the crate does.
2. **Overview paragraph** — context, the relationship to other Krabka crates, and the query language or wire format it implements.
3. **Key modules or types** — a short list with links to the main entry points.
4. **Feature flags** — if the crate has any, with a description of each and which ones are on by default.

```rust
//! `PromQL` query engine.
//!
//! `krabka-promql` parses `PromQL`, lowers the AST onto `DataFusion` plans, and
//! evaluates instant and range queries over a step grid. It reads through a
//! block-store adapter and does no HTTP of its own; `krabka-metrics-service`
//! serves the Prometheus query API above it.
//!
//! # Key Types
//!
//! - [`PromqlEngine`] — the query entry point, built from [`EngineOpts`].
//! - [`MetricBlockStore`] — the adapter that reads blocks out of `krabka-blockstore`.
//! - [`PromqlError`] — the crate error type.
//!
//! # Feature Flags
//!
//! - `experimental-functions` — the `limitk` and `limit_ratio` functions, which Prometheus still marks experimental (off by default).
```

## Public Item Documentation

Every public item that is part of the crate's published API should have a doc comment. This covers `pub fn`, `pub struct`, `pub enum`, `pub trait`, and `pub type`. Items that are not part of the published API do not need rustdoc:

- Private items (`fn`, `struct`, etc. without `pub`).
- `pub(crate)` items — visible within the crate but not to consumers.
- `pub` items inside private modules (`mod foo`, not `pub mod foo`).

Use `//` comments on these if the logic needs an explanation. Before you add rustdoc, confirm that a public module path exposes the public type. Krabka does not force the `missing_docs` lint, so this is a review expectation and not a compiler error. Enforce it at review.

### One-Liner Items

Simple items use a single `///` line:

```rust
/// The newest sample timestamp this block holds, in milliseconds.
pub max_time_ms: i64,
```

### Functions and Methods

Document what the function does, not how. Include parameters only when their purpose is not obvious from the name and the type.

```rust
/// Appends a batch of samples to the open block, returning the new row count.
///
/// Called only from the single writer task; every reader sees the block
/// through a shared reference.
pub fn append(&mut self, batch: &RecordBatch) -> Result<usize, BlockStoreError> {
```

### Complex Items

For types or functions with non-trivial behaviour, use structured sections:

```rust
/// A step grid over a query's time range.
///
/// The grid holds one evaluation timestamp per step, from `start` to `end`
/// inclusive. `PromQL` evaluates every instant selector against this grid, so
/// two queries with the same range and step align sample for sample.
///
/// # Examples
///
/// ```no_run
/// # use krabka_promql::EngineOpts;
/// # fn f(opts: &EngineOpts) {
/// let grid = opts.step_grid(0, 60_000);
/// # }
/// ```
///
/// # Panics
///
/// `step_grid` panics if the step is zero.
```

## Sections

Use only these standard sections, in this order:

| Section | When to use |
|---------|-------------|
| `# Examples` | Complex APIs where usage is not obvious |
| `# Panics` | When the function can panic in normal use |
| `# Errors` | When the function returns `Result` and the error conditions are worth a note |

There is no `# Safety` section: Krabka forbids `unsafe` (`unsafe_code = "forbid"`), so there are no `unsafe fn` to document.

The workspace lints relax `missing_errors_doc` and `missing_panics_doc`, so the lints do **not** force a `# Errors` or `# Panics` section. Add them where they genuinely help the caller. Omit them when the error or panic condition is already obvious from the signature. Do not add sections that only repeat the summary.

**State the real conditions for the item the section sits on. Never paste a generic sentence across a crate.** A section that does not describe its own item is worse than no section, because a reader trusts it. If you cannot state the real condition, delete the section. Do not leave a placeholder.

A prose audit of this workspace found eight such boilerplate strings and hundreds of copies. Many were false. One `# Panics` body named a poisoned mutex on a file with no mutex. One `# Errors` body named Kubernetes in a crate with no Kubernetes dependency. Doc comments said that encode functions fail on truncated input. One `# Errors` section sat on a function whose `Result` error type is not an error.

## Examples

- Doc examples compile **and run** in CI. `crate_tests` emits a `<crate>_doc_test` target for every library, so `bazel test //...` runs them. Keep them correct against the current API.
- Use ```` ```no_run ```` for examples that need a runtime, a network, object storage, or a live upstream service. These examples compile, but CI does not run them.
- Use ```` ```ignore ```` only for genuinely incomplete snippets, and sparingly.
- Keep examples minimal. Show the API call, not the setup. Use `#`-hidden lines for boilerplate the reader does not need to see.
- `rustfmt.toml` sets `format_code_in_doc_comments = true`, so `cargo +nightly fmt` formats the code inside your examples. Keep them fmt-clean so the format check passes.

## Upstream Specification References

When a type or function implements a specific upstream behaviour, reference the specification so a reader can trace the requirement. The upstream for each signal is Prometheus and Grafana Mimir for metrics, Grafana Loki for logs, Grafana Tempo for traces, and Grafana Pyroscope for profiles.

```rust
/// Applies the staleness rules for a range selector, as `PromQL` defines them.
///
/// [staleness]: https://prometheus.io/docs/prometheus/latest/querying/basics/#staleness
```

Where the upstream documentation does not describe the behaviour, cite the upstream source or the differential suite that pins it instead. A behaviour that only the differential suite establishes should say so, and name the suite.

Use reference-style links at the bottom of the doc comment. Do not inline long URLs in the prose.

## Cross-References

Use rustdoc syntax to link to other types and modules:

```rust
/// Returns the [`Labels`] decoded from the given row.
///
/// See [`SeriesFingerprint`] for the hash the index keys series by.
```

Use full paths when you reference an item in another crate:

```rust
/// Reads blocks through [`krabka_blockstore::BlockStore`].
```

## Configuration Structs

Every field of an operator-facing `Config` struct must have a doc comment. A field with a default value must also document that default:

```rust
/// Operator-facing service configuration.
pub struct ServiceConfig {
    /// HTTP query and ingest listen address. Default: `127.0.0.1:3100`.
    pub listen_addr: SocketAddr,

    /// Maximum concurrent block scans per query. Default: `8`.
    pub max_concurrent_scans: NonZeroUsize,
}
```

These structs derive `clap::Parser`, so each field is also a command-line flag and an environment variable. The doc line is what the operator reads in `--help`. Keep it accurate and name the default.

## Traits

Trait documentation should describe the contract, not the implementation. Include:

1. What implementors must provide.
2. What callers can expect.
3. Lifecycle, if the trait involves registration, handles, or shutdown.

```rust
/// Resolves profile matchers to a samples table over a tenant's data.
///
/// Implementors give label-matched, time-ranged access to stored profiles.
/// The flame-graph engine queries a store from several tasks at once, so a
/// backend must be safe to call concurrently.
pub trait ProfileStore: Send + Sync {
```

## Trait Implementations

Trait impl methods do not need `///` doc comments, unless the implementation behaviour is surprising or it deviates from the trait documentation. A normal `//` comment that explains the approach is useful:

```rust
impl Ord for SeriesFingerprint {
    // Ordered by the raw hash, not by label order: the index sorts blocks by
    // fingerprint, and a label-order comparison would break the merge.
    fn cmp(&self, other: &Self) -> Ordering {
```

## What Not To Document

- Re-exports (document at the source).
- `impl` blocks for derived traits (`Debug`, `Clone`, etc.).
- Trait impl methods, unless the behaviour is surprising. Use `//` comments instead.
- Test modules and test helper functions.
- Items behind `#[doc(hidden)]` and generated code. `krabka-metrics` and `krabka-profiles` generate prost types from vendored protos in their `build.rs`. Document the generated module's shape at the module level, not for each generated field.

## Checking Documentation

```bash
# Check for missing docs / broken links on public items
cargo doc --no-deps --package <crate>

# Build docs for the whole workspace
cargo doc --no-deps --workspace

# Verify examples compile and run
cargo test --package <crate> --doc
```

## Correctness Pass

After you add or update doc comments, always do a correctness pass against the code. Documentation that describes wrong behaviour is worse than no documentation.

Check for:

1. **Signature mismatches** — do parameter types and return types in docs match the code?
2. **Stale defaults** — do documented default values match the `Default` impl and the config generator?
3. **Renamed or removed items** — do cross-references point to types and methods that still exist?
4. **Example code** — would the examples compile and run against the current API?
5. **Feature flag references** — are conditional compilation features still valid?
6. **Behavioural and compatibility claims** — does the code do what the doc says, and does the referenced upstream specification still describe that behaviour?

You should do this pass whenever you write docs or refactor code. CI's `docgen` job builds the rustdoc for every crate, and the doc tests run under `bazel test //...`. Those catch a doc that does not build and an example that does not compile. Neither catches prose that describes the wrong behaviour. The correctness pass is how your hand-written rustdoc stays true.
