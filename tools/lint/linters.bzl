"""Linter aspects.

Clippy runs over the same `rust_library` / `rust_test` targets the build already
has, so it sees exactly the crates, features and dependencies the build sees --
rather than a second `cargo clippy` resolution that can disagree with it.

The aspect comes from `aspect_rules_lint_rust` rather than from `@rules_rust`
because `aspect lint` reads the `rules_lint_machine` output group, and only this
one writes it. That module registers a Rust toolchain of its own; see
//MODULE.bazel for how it coexists with this repository's.

    aspect lint                        # narrowed to changed targets, with annotations
    bazel build --config=lint //...    # everything
"""

load("@aspect_rules_lint//lint:lint_test.bzl", "lint_test")
load("@aspect_rules_lint_rust//:clippy.bzl", "lint_clippy_aspect")

clippy = lint_clippy_aspect(
    config = Label("//:clippy.toml"),
)

clippy_test = lint_test(aspect = clippy)
