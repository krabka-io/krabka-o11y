# Rust Code Style Guide

This guide defines the general Rust coding conventions for Krabka crates. It adds to the more specialised guides: [rustdoc](rustdoc_style_guide.md) for doc comments, [design docs](design_doc_style_guide.md), [READMEs](readme_style_guide.md), and [coverage reports](coverage_report_style_guide.md). The rules come from the patterns already set in `krabka-blockstore`, `krabka-promql`, `krabka-traceql`, `krabka-metrics`, and `krabka-traces`.

Reviewers expect these conventions, and they keep a change consistent with the code around it. When in doubt, match the surrounding module and write idiomatic Rust.

## Applying These Conventions

These conventions came together over time, so not all existing code obeys every rule here. That is expected. **Do not make style-only sweeps across untouched files.** Such sweeps create large diffs that are hard to review, and they churn the history for no functional gain.

Bring a file into line with this guide only when you already modify it for another reason. Keep the tidy-ups proportionate to the change, so the substantive diff stays easy to review.

## Idiomatic Rust First

Default to idiomatic, community-standard Rust. This guide records Krabka's *project-specific* conventions and the few places where the project differs from the norm. It is not a complete style manual. For everything it does not cover, follow the canonical references:

- [The Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) — the checklist for predictable, idiomatic public APIs: naming, trait implementations, and interoperability.
- [The Rust Style Guide](https://doc.rust-lang.org/style-guide/) — the formatting and layout conventions that `rustfmt` implements.
- [The Rust Book](https://doc.rust-lang.org/book/) and [Rust by Example](https://doc.rust-lang.org/rust-by-example/) — idioms and patterns.

Clippy enforces most of these idioms in practice. Under `-D warnings` with `clippy::pedantic` enabled, its suggestions are not optional. See [Linting](#linting). Prefer the idioms that the compiler and Clippy point you to:

- Express flow with iterators and `Option`/`Result` combinators such as `map`, `and_then`, `ok_or`, and `?`. Use them in place of manual index loops or nested matches, where they read more clearly.
- Implement the standard conversion and formatting traits: `From` / `TryFrom`, `Display`, `FromStr`, `Default`, and `Iterator`. Use them in place of custom `to_x` / `from_x` methods, so types compose with the ecosystem. Wire messages implement `Encode` / `Decode`. Error types convert with `#[from]`. Resources release on `Drop`.
- Accept borrowed types in function signatures, such as `&str`, `&[u8]`, and `impl AsRef<…>`, and return owned values. Do not take `String` / `Vec<T>` by value only to read from it. On the decode hot path, prefer the borrowed message flavour `crate::borrowed::*`. It references the input buffer and does not copy it.
- Use the newtype pattern to give wire and domain values distinct types, such as `ApiKey`, `NodeId`, offsets, and epochs. Do not thread raw integers through the code. The compiler then stops you when you cross two of these types. See [Newtypes for Domain Values](#newtypes-for-domain-values).
- Avoid needless `clone()` and intermediate allocations on hot paths. Borrow or move instead. The produce and fetch paths run per record batch, so an extra allocation there is a throughput regression.

## Toolchain and Edition

- The workspace pins an **exact stable toolchain** in [`rust-toolchain.toml`](../../rust-toolchain.toml): `channel = "1.97.0"` with `rustfmt` and `clippy`. Every developer and every CI build then compiles with the same compiler.
- The workspace [`Cargo.toml`](../../Cargo.toml) defines the edition and the MSRV once, in `[workspace.package]` as `edition = "2024"` and `rust-version`. Each crate inherits them with `edition.workspace = true` / `rust-version.workspace = true`. Never hard-code them in a crate's `Cargo.toml`. Check that file for the current values.
- Do not use nightly-only *language* features in crate code. If a feature is not available on the pinned stable toolchain, it is not available here.
- The **one** sanctioned use of nightly is `cargo +nightly fmt`. See [Formatting](#formatting). It is necessary because `rustfmt.toml` enables a formatting option that is still nightly-gated. That option changes only the layout of the source, never what the compiled crates depend on.

## Formatting

`rustfmt` decides the formatting, and [`rustfmt.toml`](../../rustfmt.toml) configures it:

```toml
format_code_in_doc_comments = true
```

That option keeps the code in `///` / `//!` examples in the same format as the rest of the tree. It is a nightly-only rustfmt feature at present, so CI runs:

```bash
cargo +nightly fmt --all -- --check
```

Run `cargo +nightly fmt` before you commit. Editors in the repo format on save. Do not hand-format code to make it different from what `rustfmt` produces. A mismatch fails the build.

## Linting

Clippy is a hard gate. CI runs:

```bash
cargo clippy --workspace --all-targets -- -D warnings
```

Every warning is an error, across all targets. Fix the lint. Do not suppress it. If a lint genuinely does not apply, use a narrowly-scoped `#[allow(...)]` on the specific item, not a crate-wide allow, and add a `//` comment that gives the reason.

The workspace sets lint levels **once**, in the `[workspace.lints]` table. Every crate inherits them through `[lints] workspace = true`. The current policy is:

- `unsafe_code = "forbid"` — Krabka crates contain no `unsafe`. See [Wire-Format Safety](#wire-format-safety). The single documented exception is the `krabka-log-iobench` benchmark crate. It opts out of the workspace lints because `memmap2::Mmap::map` is `unsafe` by contract, and the opt-out keeps the project-wide forbid intact. Do not use that pattern in production crates.
- `clippy::pedantic = "warn"` — the pedantic group is on, so expect Clippy to be stricter than its defaults.
- A small set of pedantic lints stay relaxed on purpose while the public API is pre-1.0: `module_name_repetitions`, `missing_errors_doc`, and `missing_panics_doc`. The workspace `Cargo.toml` documents them. Do not add to the list without a discussion first.

Do not add per-crate lint configuration. A crate that needs a different lint level almost always wants a scoped `#[allow]` on the item instead.

## Naming

Follow standard Rust naming. Clippy enforces most of these rules:

- `snake_case` for functions, methods, variables, modules, and crate features.
- `UpperCamelCase` for types, traits, and enum variants.
- `SCREAMING_SNAKE_CASE` for constants and statics.
- `Cargo.toml` names each crate `krabka-<name>`, and other crates import it as `krabka_<name>`. The directory under `crates/` is the bare `<name>`.
- Prefer descriptive names over abbreviations, but keep the upstream vocabulary intact: `series`, `label`, `matcher`, `sample`, `exemplar`, `span`, `resource`, `stream`, `profile`, `block`, and `tenant`. This is the vocabulary of the codebase and of Prometheus, Loki, Tempo, and Pyroscope. A field named to match an upstream wire field or query-language construct should keep that name.

## Imports and `use` Blocks

Organise `use` statements into up to three blocks in this order, with a single blank line between blocks:

1. The standard library — `std::…`, `core::…`, and `alloc::…` all share this one block.
2. Third-party crates — this block includes the sibling `krabka_<name>` workspace crates, because they are ordinary external dependencies of the crate that imports them: `krabka_log::…`, `krabka_protocol::…`, and `tokio::…`.
3. Local imports — `crate::…`, `super::…`, and `self::…` share this one block.

rustfmt enforces this layout. `rustfmt.toml` sets `group_imports = "StdExternalCrate"`, so `cargo +nightly fmt` produces exactly these three blocks in this order, and the CI format check keeps them that way. The option is nightly-gated, like `format_code_in_doc_comments`, and plain stable `cargo fmt` skips it without a message, so run `cargo +nightly fmt`. Omit any block that has no imports, which is why the count is "up to three". Never leave an empty block.

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;
use krabka_blockstore::{BlockStoreError, BlockWriter};
use tokio::sync::{Notify, mpsc, oneshot};

use crate::error::TracesError;
use crate::wal::SpanRecord;
```

**Collapse imports that share a leading path** into a single nested statement. Do not repeat that prefix across lines. Factor out the shared part and group the different tails in braces.

```rust
// preferred
use crate::{error::TracesError, wal::SpanRecord};

// avoid — repeats the `crate` prefix
use crate::error::TracesError;
use crate::wal::SpanRecord;
```

This is the rustfmt `imports_granularity = "Crate"` layout. `rustfmt.toml` also sets it, and `cargo +nightly fmt` applies it, so the merge is automatic. You do not have to collapse imports by hand, and you should not expand them by hand either. Avoid the opposite, Java-style extreme, where every item gets its own fully-qualified line.

**Avoid glob imports** such as `use some_crate::*;`. A glob pulls every item from a crate into scope and hides where each name comes from. When a later update to a dependency removes or renames an item, the compiler reports an undefined symbol and gives no hint which glob it came from. A new upstream item can also clash with a name from another glob, and the compiler reports nothing. Explicit imports keep that traceable for the next maintainer.

`use super::*;` is the one sanctioned glob, and even then it is the exception, not the default. Use it only in a **leaf module** or in a **unit-test module**. A leaf module is one logical module split across several deeply-interrelated files that legitimately share the parent's imports. In a unit-test module, `#[cfg(test)] mod tests { use super::*; … }` is the well-defined idiom (see [Tests](#tests)). In ordinary submodules, list imports explicitly and do not glob-import the parent.

**Do not glob enum variants into scope** with `use SomeEnum::*;`. It hides the type of the variants, and it can hide that the code handles an enum at all. This matters most for the prost-generated protobuf enums and for the query-language AST enums. Spell out `LogqlExpr::LabelReplace` in full, and do the same in `match` arms. The visible type is the point, not noise.

**Keep `use` at module scope.** Imports belong at the top of the file or of the `mod` block, never inside a function body. A reader should find every path a function depends on in one place. A function-local `use` hides where a name really comes from, and an alias hides it most of all.

**Name child modules through `self::`** when you import or re-export from them: `use self::foo::Foo;` and `pub use self::foo::Foo;`. The explicit `self::` separates the child module from a dependency of the same name. A crate called `foo` added later then cannot change what the path resolves to.

```rust
mod config;
mod error;

pub use self::config::Config;
pub use self::error::Error;
```

## Visibility

Control an item's visibility **at its own definition**, with `pub` or `pub(crate)` on the item. Do not widen or restrict visibility indirectly through re-exports or wrapper modules elsewhere. The `pub` keyword on the declaration is the single source of truth for how visible an item is.

Inside a module that is itself private or `pub(crate)`, write plain `pub`, not `pub(crate)`. The enclosing module already caps the visibility, so `pub(crate)` is redundant noise. The workspace does not enable `unreachable_pub`.

Re-export public API at the crate root where this helps a reader find it. Krabka crates commonly re-export their error type and their primary types from `lib.rs`. Document each item at its definition, not at the re-export.

## Error Handling

Crates and subsystems define their own error enums with [`thiserror`](https://docs.rs/thiserror). Name each one `<Subsystem>Error` and re-export it from the crate root, as in `BlockStoreError`, `PromqlError`, `TraceqlError`, `TracesError`, and `ProfileError`:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum TracesError {
    #[error("block store: {0}")]
    BlockStore(#[from] krabka_blockstore::BlockStoreError),

    /// The query named a tenant that has no blocks in the requested window.
    #[error("unknown tenant: {tenant}")]
    UnknownTenant { tenant: String },
}
```

- Give every variant an `#[error("…")]` message. Add a `///` doc line where the variant's meaning is not already obvious from that message.
- Mark public error enums `#[non_exhaustive]`, so a new variant is not a breaking change. Krabka is pre-1.0, but this keeps a `match` on an error honest about the open set.
- Prefer **focused, leaf error types** for self-contained sub-parsers, instead of the crate-root `Error`. A small decoder that returns the whole crate's error enum leaks unrelated variants into its signature.
- Use `?` to propagate an error. Convert between error types with `#[from]` or an explicit `map_err`. Do not convert through a string.
- Avoid `.unwrap()` / `.expect()` on fallible paths in library code. They are acceptable only where an invariant guarantees success. In that case, use `.expect("reason the invariant holds")` so the message documents the invariant. Tests may unwrap freely.
- Never `panic!` in response to malformed wire input or a malformed query string. Decoders and parsers return errors. This is a security property, and property tests and fuzzing verify it (see [Wire-Format Safety](#wire-format-safety)).

The workspace lints relax `missing_errors_doc` and `missing_panics_doc`, so the `# Errors` / `# Panics` rustdoc sections are *encouraged where they add value* and no lint enforces them. See the [rustdoc guide](rustdoc_style_guide.md).

## Wire-Format Safety

Krabka forbids `unsafe` across the workspace with `unsafe_code = "forbid"`, so no raw-pointer or transmute hazard needs a guard. The safety concern that remains is **untrusted wire input**. Every ingest path here decodes bytes that an arbitrary agent sends: Prometheus remote-write protobuf, OTLP trace protobuf, and pprof profiles. Every query path parses a string that an arbitrary caller sends. A malicious or defective client can put any value in a length or count field, and any text in a query.

**Never size an allocation directly from a wire-supplied length or element count.** A `Vec::with_capacity(n)` where `n` came off the wire is a denial-of-service vector. One small request that declares a four-billion-element array can exhaust the memory. Validate the value against a sane bound first, then convert it with `try_from` and handle the error, not with `as`:

```rust
if count > MAX_SERIES_PER_REQUEST {
    return Err(WireError::TooManySeries(count));
}
let count = usize::try_from(count).map_err(|_| WireError::TooManySeries(count))?;
```

A quiet `as usize` truncation on client-controlled input is a parsing bug and a potential vulnerability, and so is an unbounded `with_capacity`. Decoders return errors on such input. They do not panic, and they do not allocate memory at the request of an attacker. The same rule covers a symbol-table index, a label count, and a sample count read out of a remote-write or OTLP message.

## Newtypes for Domain Values

A raw `i32` or `i64` carries no meaning, and this domain is full of same-typed values where a mix-up is catastrophic. A query start, a query end, and a block boundary are all epoch-nanosecond `i64`s. A metrics step is an `i64` too, but it is a duration and not an instant. A Jaeger trace id is a `(high, low)` pair of `i64`s, and a WAL offset range is a `(min, max)` pair. A function that takes two or three of these as bare integers still compiles when a caller transposes the arguments. The bug then appears only at run time, as an empty result window, or as a trace fetched under a swapped id.

**Wrap a domain value in a newtype when a mix-up with another value of the same primitive type would be a real bug.** The compiler then rejects the transposition at the call site, and the mistake never becomes a production incident. This follows the [newtype-safety guidance](https://github.com/leonardomso/rust-skills/blob/master/rules/api-newtype-safety.md) and the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/). Krabka already does it in `krabka-traces` for `UnixNano`, `WindowStartNs`, `TraceIdHigh`, `TraceIdLow`, `MinOffset`, and `MaxOffset`, in `krabka-traceql` for `UnixNano` and `DurationNanos`, and it uses `Offset` and `PartitionIndex` from `krabka-ids`. This section makes a newtype the default for new domain values.

```rust
use derive_more::{Display, From, Into};

/// An absolute epoch-nanosecond instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct UnixNano(pub i64);

/// A nanosecond duration between range-query buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Display, From, Into)]
pub struct DurationNanos(pub i64);

// Signature that no longer accepts a transposed call:
fn range_query(start: UnixNano, end: UnixNano, step: DurationNanos) { /* … */ }
```

Guidance:

- **Where the confusion risk is real, prefer a newtype over a bare primitive.** This matters most for functions and constructors that take **two or more parameters of the same primitive type** with different meanings. That is the swap bug the newtype exists to prevent.
- **Split the derives by origin.** The identity and ordering traits come from the standard library. Use `Copy, Clone, PartialEq, Eq, Hash` for anything used as a `HashMap` key, and add `PartialOrd, Ord` for ordered values such as offsets and timestamps. Keep id newtypes `Copy` (see [own-copy-small](https://github.com/leonardomso/rust-skills/blob/master/rules/own-copy-small.md)).
- **Use [`derive_more`](https://lib.rs/crates/derive_more) for the wrapper boilerplate.** Do not hand-write these impls. It is a workspace dependency, declared with `derive_more.workspace = true`. Use:
  - `Display` — so the value logs as the bare inner primitive without an explicit `.0` at every `tracing` call. Write a manual `impl Display` only when the value needs special formatting, for example `ORD-{:08}`.
  - `From` / `Into` — the explicit, visible conversions to and from the inner primitive. Use them at the [wire boundary](#newtypes-for-domain-values), as in `UnixNano::from(raw)` and `let raw: i64 = ts.into()`.
  - `FromStr` — for ids parsed from config or CLI args.
  - `Deref` / `AsRef` — use these sparingly, only where transparent access to the inner value genuinely reads better. A broad `Deref` can weaken the type distinction the newtype exists for, so prefer an explicit accessor or `Into` in most cases.
  - `Add` / `Sub` / `AddAssign` / `Sum` — for values with real arithmetic, such as an advance of an `Offset` or a sum of byte counts. Do not derive arithmetic on ids, where `id + id` has no meaning.
  - `Constructor` — a `Foo::new(inner)` when you want a named constructor but no validation.

  ```rust
  use derive_more::{Display, From, FromStr, Into};

  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, From, Into, FromStr)]
  pub struct WindowStartNs(pub i64);
  ```
- **Use `#[serde(transparent)]`** on newtypes that are serialised, so the wire or JSON encoding is exactly the inner primitive and never a wrapper object.
- **Comparison against the inner primitive is allowed. Nothing else is relaxed.** The shared `krabka-ids` identifiers hand-implement `PartialEq`/`PartialOrd` against their inner primitive in both directions. A value check such as `offset >= 0` then reads without an explicit `.0`. This exception is deliberately narrow. It does **not** let you pass the newtype where its primitive is expected, and it does **not** let the newtype act as its primitive in a map key. It also does **not** let you compare it to a *different* newtype, so the swap-bug safety is intact. Expose a sentinel value as a **named constant**, as `krabka-ids` does with `Offset::ZERO`. The comparison then reads as intent rather than a magic number.
- **Validate in the constructor** for newtypes over `String` or other unconstrained inputs, such as a tenant id or a validated label name. Expose `fn new(..) -> Result<Self, _>` and an `as_str` or other accessor. Do **not** also derive `From`, because an infallible `From` would bypass the validation. An instance should be proof that the value is well-formed. This is [parse, don't validate](https://github.com/leonardomso/rust-skills/blob/master/rules/api-parse-dont-validate.md).
- **The newtype is zero-cost.** It has the same size and layout as the primitive, so there is no runtime reason to avoid one.

`derive_more` is also the right tool anywhere else it removes hand-written boilerplate. Examples are a `Display`, `From`, or `Constructor` on a small domain struct, and `From` conversions between related types. It is not a licence to derive `Deref` broadly, and it is not a licence to replace the `thiserror`-based error enums. Those stay on `thiserror` (see [Error Handling](#error-handling)).

**The wire boundary is the exception.** `krabka-metrics` and `krabka-profiles` generate prost types from vendored protos in their `build.rs`, and those types must keep the upstream message shape. The serialised WAL records and the Arrow columns are the same kind of boundary. Do **not** newtype a generated field or a stored column, and do not hand-edit generated code. Newtypes belong in the **hand-written layer**: the ingest path, the planner, the querier, and the block store. Convert at the boundary: read the raw integer out of a decoded message, wrap it in the domain newtype, and unwrap it back to the primitive when you write a row or encode a response. A `From`/`Into` or an `as_wire()` accessor keeps that conversion explicit and in one place.

**Don't newtype for its own sake.** A value used in a single place, with no other same-typed value in scope to confuse it with, does not need a wrapper. A `struct X(i32)` around a lone loop counter is noise. The test is whether a *mix-up is possible and would be a bug*, not whether a primitive appears.

## Dimensioned Values

A newtype separates two values that share a primitive. It does nothing about a value whose *unit* is wrong. `query_timeout_ms` and `retention_ms` are both durations, and a newtype for each still lets the code store seconds where it meant milliseconds.

**A magnitude with a unit is a `krabka-units` quantity, not a bare number.** Sizes are `ByteSize`, throughputs are `ByteRate`, timeouts and intervals and retention windows are `Time`, event rates are `Frequency`, and fractions are `Ratio`. These are [`uom`](https://docs.rs/uom) quantities, so a unit conversion is a method call and not a hand-written `* 1024`. The compiler also checks arithmetic across dimensions: `ByteSize / Time` is a `ByteRate` and nothing else.

```rust
use krabka_units::prelude::*;

let quota: ByteRate = mebibytes_per_sec(10);
let drain: Time = quota.time_to_transfer(mebibytes(50));
```

The same two exceptions apply as for newtypes, plus one more. The **generated wire types stay raw**. Convert at the hand-written boundary with the extension traits in `krabka_units::convert`. **Instants are not magnitudes.** An offset or an epoch-nanosecond timestamp is a coordinate and stays a newtype, and `Time` is always an *extent*. And **dimensionless counts stay integers**: a series count or a retry budget has no unit to get wrong.

Drop the unit from the name once the type carries it. `max_block_bytes: i64` becomes `max_block: ByteSize`. Keep the suffix only where the name matches an upstream config key or a wire field that is still a raw integer, as `start_ns` and `end_ns` do.

## Feature Flags

Krabka is not `no_std`. Every crate here runs on `std`, and the five services are ordinary `std` binaries. The flags in this workspace are few, and each one exists for a reason:

- `vendored-protoc` — on by default in `krabka-metrics` and `krabka-profiles`. It supplies `protoc` through the `protoc-bin-vendored` crate for a plain `cargo build`. Bazel supplies its own `protoc` and turns the flag off, because the vendored crate bakes an absolute build path into the artifact and a sandboxed build refuses that.
- `heap-profiling` — off by default in the five service crates. It turns on the jemalloc profiling allocator.
- `experimental-functions` — off by default in `krabka-promql`. It enables the `PromQL` functions that Prometheus still marks experimental, and the conformance corpus skips the file that needs them unless it is on.

For feature flags generally:

- Features must be **additive**. A feature that is on must not break or change the behaviour of another feature. Code must compile under the feature combinations CI exercises, and with default features.
- Gate optional integrations behind features, and do not make them mandatory dependencies. This covers profiling allocators, build-tool fallbacks, and backend adapters.
- Document every feature flag in the crate-level doc comment, and state which flags are on by default. See the [rustdoc guide](rustdoc_style_guide.md).

## Comments

- Doc comments, `///` and `//!`, document the public API. See the [rustdoc guide](rustdoc_style_guide.md). Public items should carry them. Private and `pub(crate)` items do not need them.
- Use `//` line comments for non-obvious private logic and for surprising trait-impl behaviour.
- **Comments describe the present state of the code, not its history.** Do not write porting narration such as "moved from X", "replaces the old Y", or "now takes Z". Git holds that history, and a stale migration note misleads the reader. Krabka is greenfield and undeployed, so there is no old version to reference (see [`CLAUDE.md`](../../CLAUDE.md)).
- Explain *why*, not *what*, when the *what* is already clear from the code. A comment that ties a branch to a specific upstream behaviour, or to a difference a differential suite found, earns its place. A comment that restates the `if` condition does not.

## Async and Concurrency

Krabka is built on `tokio`, which `[workspace.dependencies]` pins.

- Prefer the `tokio::sync` primitives for anything that coordinates across `.await` points: `mpsc`, `oneshot`, `Notify`, `RwLock`, and the async `Mutex`.
- **Never hold a blocking lock across an `.await`.** `std::sync::Mutex` is fine for an O(1) synchronous critical section on a hot path, and Krabka uses it that way to guard an in-memory scan cache. If the critical section awaits or does real work, use `tokio::sync::Mutex` instead. A `std` lock held across an await can deadlock the runtime.
- Structure long-lived work as tasks with an explicit shutdown, not as detached fire-and-forget tasks. Every service binary handles SIGTERM and shuts its tasks down. The **single-writer-task** pattern is the established shape for serialised writes. One task owns a resource and drains an `mpsc` channel of commands, and callers send messages and await a `oneshot` reply. Match that pattern for new serialised-mutation components.
- Spawn blocking or CPU-bound work with `spawn_blocking`, so it does not stall the async worker threads. This covers compression, symbolisation, and Parquet encode.

## Logging and Observability

- Use the `tracing` macros such as `tracing::debug!`, `error!`, and `instrument` for logs and spans. Do not use `println!` / `eprintln!` in library or server code.
- Every service binary calls `krabka_telemetry::init` to install the shared subscriber and the OTLP export. Do not build a subscriber or a formatter by hand in a service.
- Keep log levels meaningful: `error!` for faults that need attention, `warn!` for recoverable anomalies, and `debug!`/`trace!` for diagnostics. Do not log per sample, per span, or per request at `info!` on the ingest and query hot paths.

## Cargo and Dependencies

- Inherit the shared package metadata from the workspace with `.workspace = true`: `edition`, `rust-version`, `license`, and `authors`. Inherit the lint policy with `[lints] workspace = true`.
- Krabka **does** use a `[workspace.dependencies]` table. Declare a shared dependency once there, with its version and features, and reference it from a crate with `<dep>.workspace = true`. This keeps versions consistent across the ten crates, and it holds the cross-crate version pins.
- Several pins are **lock-stepped** and carry an explanatory comment in the workspace `Cargo.toml`. The `datafusion` git revision moves with `arrow`, `parquet`, and `object_store`. Do not bump one of these alone. Read the comment and move the whole set together.
- The sibling `krabka-*` crates come from `[patch.crates-io]`, and the `version = "0.4.0"` beside each one is a formality. Renovate is configured to leave them alone, and the `sync-siblings` workflow moves the revisions instead.
- Keep dependencies minimal and justified. Every new third-party crate passes through the `cargo deny` gate in `deny.toml`, which the `ci` workflow runs. Check a sibling crate or the lockfile before you introduce a new version of something already in the tree.

## Tests

Where a test lives depends on what it needs to reach:

- **Integration tests — the default for public behaviour.** If a test exercises a crate's public API, put it in the crate's `tests/` directory, a sibling of `src/`. These compile as separate crates and can see only the public surface, which keeps them honest about what the crate actually exposes.
- **In-file unit tests — only for private access.** Add a `#[cfg(test)] mod tests { use super::*; … }` block at the bottom of a source file *only* when the test needs access to private module or file internals that are not public and should not be public. This is the one place `use super::*;` is expected. When such a test module grows large next to the source it covers, move it into a dedicated `tests.rs` file in the module, declared with `#[cfg(test)] mod tests;`. It then does not dominate the source file, and as a child module it keeps the same private access.

Conventions for both:

- Test functions are `snake_case` and name the scenario under test. Do not add rustdoc to test functions or test helpers.
- Use [`assert2`](https://docs.rs/assert2) and its `assert!` / `check!` macros for assertions. It is the workspace-wide standard, and it gives captured-expression diagnostics on a failure.
- Wire-facing and codec code carries **property tests** written with `proptest`, and these are usually encode and decode round-trips. File-backed corpora run through [`datatest-stable`](https://docs.rs/datatest-stable), so each fixture is scheduled as its own test process. New parsing code should come with round-trip coverage.
- Put a trait seam in front of a store, and give the test an in-memory fake behind it. A unit test can then exercise IO-decision logic without object storage and without a live service.
- **Differential tests against the real upstream component** check the behaviour that must match Prometheus, Grafana Mimir, Grafana Loki, Grafana Tempo, or Grafana Pyroscope. Six such suites exist. Each one boots the upstream image with `testcontainers` and compares against it, so they are `#[ignore]`d for `cargo` and tagged `docker` for Bazel. Run one with `bazel test --config=docker //crates/<name>:<stem>_docker_test`.
- The test suite runs under `cargo nextest`, and **mutation testing** runs with `cargo mutants`. A surviving mutant means that a line runs but that no test asserts on its behaviour. Write the assertion that kills it. The full per-crate sweep is `bazel test //crates/<name>:<name>_mutants`, and it runs from the scheduled job rather than on a pull request.

See the root [`README.md`](../../README.md) for how to run each tier: the workspace build, the container suites, and the mutation sweep. See the [coverage report guide](coverage_report_style_guide.md) for how the project reports coverage per crate.

## Markdown and Prose (for docs you write)

- **Write in ASD-STE100 Simplified Technical English.** See the [prose style guide](prose_style_guide.md). It covers word choice, sentence length, active voice, and the rule that `must` marks only a requirement the code enforces. It applies to comments and doc comments as well as to Markdown.
- **One line per paragraph — do not hard-wrap at 80 columns.** Let the renderer wrap. Some older Krabka docs are hard-wrapped. That is not a reason to reflow them, but new prose should follow this rule.
- Follow the relevant guide for the document type: [design docs](design_doc_style_guide.md), [READMEs](readme_style_guide.md), [coverage reports](coverage_report_style_guide.md).
