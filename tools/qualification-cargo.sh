#!/usr/bin/env bash
# Replay every container case skipped by Cargo through the pinned image harness.
set -euo pipefail
cargo test --workspace --locked
suites='attr(tags, "docker", //crates/...) except attr(tags, "scale", //crates/...)'
selection="$(bazel query "tests(${suites})")"
mapfile -t targets <<<"${selection}"
((${#targets[@]} > 0))
python3 tools/build-query-runners.py --bazel test --config=docker \
    --nocache_test_results --flaky_test_attempts=1 --test_output=all "${targets[@]}"
