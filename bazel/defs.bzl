"""Bazel macros for this repository's Cargo workspace members.

Every knob these macros need — crate name, edition, feature set, dependency
labels, workspace lints — already exists in the `@crates` repo that
`crate.from_cargo` generates from `Cargo.toml`/`Cargo.lock`. Reading it from
there rather than restating it per crate keeps the BUILD files from drifting
away from the manifests Cargo still resolves.
"""

load("@crates//:data.bzl", "DEP_DATA")
load("@crates//:defs.bzl", "all_crate_deps", "crate_name", "edition")
load("@rules_rs//rs:cargo_build_script.bzl", "cargo_build_script")
load("@rules_rs//rs:rust_binary.bzl", "rust_binary")
load("@rules_rs//rs:rust_library.bzl", "rust_library")
load("@rules_rs//rs:rust_test.bzl", "rust_test")
load("@rules_rs_mutants//mutants:cargo_mutants_test.bzl", "cargo_mutants_test")
load("@rules_rust//rust:defs.bzl", "rust_doc", "rust_doc_test")
load("@rules_shell//shell:sh_test.bzl", "sh_test")
load("//tools/lint:linters.bzl", "clippy_test")

# `[workspace.lints.rust] unsafe_code = "forbid"`. rules_rs 0.0.106 does not
# yet plumb Cargo lint tables into the Bazel build, and this is the one lint
# in that table whose guarantee must not lapse under a second build system.
# The clippy tables stay a Cargo-side gate: clippy runs as an aspect here, not
# as part of a normal build.
WORKSPACE_RUSTC_FLAGS = ["-Funsafe_code"]

def _features():
    return DEP_DATA[native.package_name()]["crate_features"]

def _aliases(kinds):
    """The renamed-dependency map, narrowed to one set of dependency kinds.

    `@crates//:defs.bzl`'s `aliases()` returns one map covering normal *and*
    dev dependencies. rules_rust treats every key of `aliases` as a dependency,
    so handing the whole map to a `rust_library` links that crate's dev
    dependencies into the library. Where two crates dev-depend on each other --
    `krabka-client-consumer` and `krabka-client-producer` do -- that is a
    dependency cycle Bazel refuses to build, and everywhere else it is dead
    weight. Cargo has no such problem: a lib and its test binaries are separate
    compilations.
    """
    data = DEP_DATA[native.package_name()]
    labels = {}
    for kind in kinds:
        for dep in data.get(kind, []):
            labels[dep] = True
        for platform_deps in data.get(kind + "_by_platform", {}).values():
            for dep in platform_deps:
                labels[dep] = True
    return {
        label: name
        for label, name in data["aliases"].items()
        if label in labels
    }

def crate_library(
        name,
        srcs = None,
        build_script_data = None,
        build_script_compile_data = None,
        **kwargs):
    """`rust_library` for a workspace member, configured from Cargo metadata.

    Args:
      name: the library target name, matching the crate's directory.
      srcs: sources; defaults to `src/**/*.rs` less `src/bin/**`.
      build_script_data: files the crate's `build.rs` reads at run time,
        e.g. `.proto` sources. Only meaningful when the crate has one.
      build_script_compile_data: files the crate's `build.rs` reaches with
        `include_str!`/`include_bytes!`, which are read while it compiles
        rather than while it runs.
      **kwargs: passed through to `rust_library`.
    """
    deps = all_crate_deps(normal = True)

    # A crate with a `build.rs` gets one, wired so its `OUT_DIR` reaches the
    # library. Four crates here generate prost types from vendored protos and
    # `include!` them from `OUT_DIR`; without this the include has no directory
    # to read and the generated modules are simply absent.
    if native.glob(["build.rs"], allow_empty = True):
        script = name + "_build_script"

        # `protoc` comes from the build, not from a vendored crate. The
        # `protoc-bin-vendored-*` crates locate their binary through
        # `env!("CARGO_MANIFEST_DIR")`, which bakes an absolute build path into
        # the artifact -- the same sources would produce different bytes on
        # different machines, and the sandbox rejects it. They cannot be read at
        # run time either: the path they want belongs to their own manifest, and
        # nothing sets it. So the feature that pulls them in is dropped here and
        # `PROTOC` is handed over instead, which is what `build.rs` prefers.
        # Cargo keeps the feature on by default and behaves as it always did.
        cargo_build_script(
            name = script,
            srcs = ["build.rs"],
            build_script_env = {"PROTOC": "$(execpath //bazel:protoc)"},
            crate_features = [f for f in _features() if f != "vendored-protoc"],
            crate_name = crate_name() + "_build_script",
            compile_data = build_script_compile_data or [],
            data = build_script_data or [],
            edition = edition(),
            tools = ["//bazel:protoc"],
            deps = [
                dep
                for dep in all_crate_deps(build = True)
                if "protoc-bin-vendored" not in dep
            ],
        )
        deps = deps + [":" + script]

    rust_library(
        name = name,
        srcs = srcs if srcs != None else native.glob(
            ["src/**/*.rs"],
            exclude = ["src/bin/**"],
        ),
        aliases = _aliases(["deps"]),
        crate_features = _features(),
        crate_name = crate_name(),
        edition = edition(),
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = deps,
        **kwargs
    )

    # Clippy as a test, so `bazel test //...` gates on it the way `cargo clippy
    # -- -D warnings` used to. The aspect alone only writes a report: it is
    # `lint_test` that turns a finding into a failure.
    clippy_test(
        name = name + "_clippy",
        srcs = [":" + name],
    )

    # `bazel build //crates/<x>:<x>_doc` renders this crate's rustdoc. The
    # rustdoc examples themselves are run by `crate_tests`, which emits a
    # `rust_doc_test`; this is the HTML.
    rust_doc(
        name = name + "_doc",
        crate = ":" + name,
    )

def crate_binary(name, crate_root, lib, tests = True, **kwargs):
    """`rust_binary` for a `[[bin]]` target that links its own crate's library.

    Args:
      name: the binary target name, matching Cargo's `[[bin]] name`.
      crate_root: the binary's entry point, e.g. `src/bin/broker/main.rs`.
        Every `.rs` file beside it is one of the binary's modules.
      lib: the `crate_library` target in this package that it links.
      tests: emit a `rust_test` over the binary's own `#[cfg(test)]` module.
        `cargo test` runs those; without this they are simply not run.
      **kwargs: passed through to `rust_binary`.
    """
    rust_binary(
        name = name,
        # The binary owns the directory holding its crate root, so its own
        # modules live beside it there.
        srcs = [crate_root] + native.glob(
            [crate_root.rsplit("/", 1)[0] + "/**/*.rs"],
            exclude = [crate_root],
            allow_empty = True,
        ),
        aliases = _aliases(["deps"]),
        crate_features = _features(),
        crate_root = crate_root,
        edition = edition(),
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True) + [lib],
        **kwargs
    )

    if tests:
        rust_test(
            name = name + "_test",
            aliases = _aliases(["deps", "dev_deps"]),
            crate = ":" + name,
            crate_features = _features(),
            edition = edition(),
            rustc_flags = WORKSPACE_RUSTC_FLAGS,
            deps = all_crate_deps(normal_dev = True),
        )

def crate_tests(
        lib,
        data = None,
        compile_data = None,
        cpu_heavy = [],
        docker = {},
        env = {},
        extra_srcs = {},
        rustc_env = {},
        manual = [],
        no_harness = [],
        doc_tests = True,
        mutants = True,
        mutants_jobs = 4,
        mutants_shards = 8,
        mutants_timeout = "long",
        unit_tags = []):
    """Unit tests, one target per `tests/*.rs`, and a mutation sweep.

    Args:
      lib: the `crate_library` target name in this package.
      data: runtime files every integration test gets (fixtures, corpora).
      compile_data: files reachable from `include!`/`include_str!` at compile time.
      env: runtime environment for every test target in the package.
      extra_srcs: per-stem sources from outside this package, for a suite that
        reaches one with `#[path]`. Bazel places a label at its own workspace
        path, which is the path such an include is written against.
      rustc_env: extra compile-time environment, e.g. `CARGO_MANIFEST_DIR`.
      manual: test stems to tag `manual` — Docker-driven or otherwise
        non-hermetic suites, the Bazel equivalent of their `#[ignore]`.
      no_harness: test stems declared `harness = false` in Cargo.toml.
      doc_tests: whether to emit a `rust_doc_test`. `cargo test` runs rustdoc
        examples; without this they are simply not run.
      mutants: whether to emit a `cargo_mutants_test`. The sweep runs the
        unit tests *and* the suites under tests/ against every mutant; the
        latter carry most of the coverage in several of these crates.
      mutants_jobs: mutants built and tested concurrently within one shard.
      mutants_shards: Bazel shards the sweep is split across.
      mutants_timeout: Bazel timeout for one shard of the sweep.
      unit_tags: extra tags for the unit-test target.
    """
    unit = lib + "_test"
    rust_test(
        name = unit,
        aliases = _aliases(["deps", "dev_deps"]),
        crate = ":" + lib,
        compile_data = compile_data or [],
        crate_features = _features(),
        data = data or [],
        edition = edition(),
        env = env,
        rustc_env = rustc_env,
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        tags = unit_tags,
        deps = all_crate_deps(normal_dev = True),
    )

    if doc_tests:
        rust_doc_test(
            name = lib + "_doc_test",
            crate = ":" + lib,
            deps = all_crate_deps(normal_dev = True),
        )

    if mutants:
        # `manual`: a full sweep rebuilds the crate once per mutant, so it runs
        # from the nightly job rather than on every `bazel test //...`.
        # Suites that need Docker, or are otherwise non-hermetic, stay out: a
        # sweep reruns them once per mutant, and `manual` is exactly the marker
        # for the ones that cannot bear that.
        integration = [
            ":" + src[len("tests/"):-len(".rs")] + "_test"
            for src in native.glob(["tests/*.rs"], allow_empty = True)
            if src[len("tests/"):-len(".rs")] not in manual and
               src[len("tests/"):-len(".rs")] not in docker
        ]

        cargo_mutants_test(
            name = lib + "_mutants",
            # Every mutant is a full rebuild of the crate plus a test run, so a
            # sweep is long by nature -- but it must still be bounded. `eternal`
            # is 3600s per shard, which for the small crates is not a timeout at
            # all, it is an hour of a runner before anyone learns something is
            # wrong. `long` is 900s; crates that legitimately need more say so.
            timeout = mutants_timeout,
            # The suites under tests/ carry most of the coverage in several of
            # these crates, and a mutant they catch is not a gap. Without them
            # logql's syntax.rs reported 165 survivors where the real number is
            # 1 -- so they are rebuilt and rerun against every mutant.
            integration_tests = integration,
            library = ":" + lib,
            jobs = mutants_jobs,
            shard_count = mutants_shards,
            tags = ["manual"],
            test = ":" + unit,
        )

    # Shared helper modules live in `tests/<name>/mod.rs` and are declared with
    # `mod <name>;` by whichever suites need them, so every integration test
    # gets them in `srcs` and names its own file as the crate root.
    helpers = native.glob(
        ["tests/**/*.rs"],
        exclude = ["tests/*.rs"],
        allow_empty = True,
    )

    for src in native.glob(["tests/*.rs"], allow_empty = True):
        stem = src[len("tests/"):-len(".rs")]
        rust_test(
            name = stem + "_test",
            srcs = [src] + helpers + extra_srcs.get(stem, []),
            crate_root = src,
            aliases = _aliases(["deps", "dev_deps"]),
            compile_data = compile_data or [],
            crate_features = _features(),
            data = data or [],
            edition = edition(),
            env = env,
            rustc_env = rustc_env,
            rustc_flags = WORKSPACE_RUSTC_FLAGS,
            # `flaky`: these assert on wall-clock behaviour, so a loaded runner
            # can fail them without the code being wrong. `cpu:4` keeps Bazel
            # from creating that load locally, but a 4-core CI runner has no
            # headroom to give. A retry separates a timing hiccup from a real
            # break, and Bazel reports the result as FLAKY rather than passing it
            # off as a clean run -- so the flakiness stays visible instead of
            # being hidden by a `#[ignore]`.
            flaky = stem in cpu_heavy,
            tags = (["manual"] if stem in manual else []) +
                   (["cpu:4", "timing-sensitive"] if stem in cpu_heavy else []),
            use_libtest_harness = stem not in no_harness,
            # One call, not two concatenated: an integration test links the
            # crate's normal *and* dev dependencies, and several crates list the
            # same package in both tables. `all_crate_deps` merges the two specs
            # through a set, so asking for both at once dedupes them.
            deps = all_crate_deps(normal = True, normal_dev = True) + [":" + lib],
        )

        if stem not in docker:
            continue

        # The same sources built again, this time to be driven by a wrapper that
        # loads Bazel's digest-pinned Kafka images before handing over. Built as
        # a non-test target so `bazel test //...` does not try to run it bare.
        rust_test(
            name = stem + "_docker_bin",
            srcs = [src] + helpers + extra_srcs.get(stem, []),
            crate_root = src,
            aliases = _aliases(["deps", "dev_deps"]),
            compile_data = compile_data or [],
            crate_features = _features(),
            data = data or [],
            edition = edition(),
            env = env,
            rustc_env = rustc_env,
            rustc_flags = WORKSPACE_RUSTC_FLAGS,
            tags = ["manual"],
            use_libtest_harness = stem not in no_harness,
            deps = all_crate_deps(normal = True, normal_dev = True) + [":" + lib],
        )

        image_tars = ["//bazel/images:%s_tar" % image for image in docker[stem]]

        # The Docker daemon is the one thing here Bazel cannot own, so it is the
        # one thing left undeclared: `no-sandbox` for the socket, and `external`
        # so a pass is never cached against inputs that do not describe the
        # daemon's state. Everything else -- the image bytes above all -- is a
        # declared, digest-pinned input rather than a mid-test network fetch.
        sh_test(
            name = stem + "_docker_test",
            size = "enormous",
            # These form real Kafka clusters and assert that a leader is elected
            # and an ISR populated inside a timeout. On a loaded runner that can
            # miss without the code being wrong -- `jvm_kip320_divergence` failed
            # one job and passed another on the same commit, with `Leader: none`
            # and an empty ISR. A retry separates that from a real break, and
            # Bazel reports FLAKY rather than PASSED, so it stays visible.
            flaky = True,
            srcs = ["//bazel:docker_test.sh"],
            args = ["$(rootpath :%s_docker_bin)" % stem],
            # The caller's `data` comes along because its `env` may name those
            # labels in a `$(rootpath)`, which only resolves for a declared
            # prerequisite of this rule.
            data = [":%s_docker_bin" % stem] + image_tars + (data or []) + [
            ],
            env = dict(env, KRABKA_IMAGE_TARS = ":".join([
                "$(rootpath %s)" % tar
                for tar in image_tars
            ])),
            tags = [
                "docker",
                "external",
                "no-sandbox",
            ],
        )
