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
cargo check --workspace --all-targets --locked
```

PRs and pushes to `main` run one non-container Bazel test pass, plus Cargo
compilation and feature checks. The main Bazel lane's lint, build, and test
commands run on BuildBuddy remote runners using the organization-level
`BUILDBUDDY_API_KEY` secret. Each invocation uses the exact checked-out commit,
16 CPUs and 64 GB of memory, with a 55-minute timeout. Fork PRs run these commands
locally because GitHub does not share organization secrets with forks. A missing
key on a trusted run fails with a configuration error.

Cargo tests, coverage, and the fuzz/benchmark
compile checks run nightly or through manual CI dispatch.

Require the **CI gate** check in branch protection. It waits for the routine
build/test/lint, Cargo, dependency policy, tool, documentation, and website jobs,
and fails if any of them fails, is cancelled, or is skipped. Nightly and release
suites are outside this PR gate.

The `image` workflow requires the full Docker, scale, proof, Kubernetes, and
strict query-conformance suites before publishing a `v*` release. Resuming a
release tests the requested source tag too. These suites do not run on PRs,
main pushes, or the nightly CI schedule. Use the **release tests** workflow's
manual dispatch to qualify a tag or commit without publishing.

Run the Docker-tagged upstream suites locally when a change affects compatibility:

```bash
bazel test --config=docker //crates/...
```

Mutation sweeps do not run in CI. Run a focused sweep by hand with:

```bash
bazel test //crates/<name>:<name>_mutants
tools/mutants-ratchet.py <name>
```

The ratchet rejects incomplete shard output before it reads the survivor count.
To seed or change a baseline, follow "Seeding a crate by hand" in
[README.md](README.md#seeding-a-crate-by-hand).

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
