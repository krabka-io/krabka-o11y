# Contributing to krabka-o11y

Thank you for improving Krabka.

## Before you change code

Open an issue for a new compatibility surface or architecture change.

Keep changes focused and follow an existing crate pattern before adding a new abstraction or dependency.

Do not add compatibility shims unless the issue requires one.

## Build and test

Run the repository checks before you open a pull request:

```bash
bazel run //tools/format
bazel test //...
cargo test --workspace --locked
```

Run the Docker-tagged upstream suites when a change affects compatibility:

```bash
bazel test --config=docker //crates/...
```

Run a focused mutation sweep with:

```bash
bazel test //crates/<name>:<name>_mutants
tools/mutants-ratchet.py <name>
```

The ratchet rejects incomplete shard output before it reads the survivor count.

## Code conventions

The workspace forbids `unsafe` code and treats Clippy warnings as errors.

Fix a warning instead of adding `#[allow(clippy::...)]`.

Use `assert2::assert!` and `assert2::check!` in tests instead of the standard assertion macros.

Tests must exercise behavior through a public or crate-visible seam.

Do not test implementation text with `include_str!` or source-file reads.

Follow the guides under [`docs/style_guides/`](docs/style_guides).

## Commits and pull requests

Use Conventional Commit subjects such as `feat:`, `fix:`, `docs:`, `test:`, or `chore:`.

Explain the user-visible compatibility boundary and name the verification you ran.

Complete the pull request checklist and link the issue it resolves.
