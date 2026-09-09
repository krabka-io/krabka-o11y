#!/usr/bin/env bash
#
# Fails when a crate's `[[test]] harness = false` declarations and the
# `no_harness` list in its `crate_tests` call disagree.
#
#   no_harness_check.sh <Cargo.toml> [stem ...]
#
# The two are separate statements of the same fact, and nothing but this
# connects them. Cargo reads the manifest; Bazel reads the BUILD file. A stem
# that is `harness = false` in one and absent from the other builds under the
# libtest harness, where rustc writes its own `main` and the suite's real entry
# point -- a `datatest_stable::harness!` in every case here -- becomes dead
# code. The target then passes having run nothing, which is the failure mode
# this exists to prevent: three conformance corpora sat in that state.
#
# `cargo metadata` does not report `harness`, and `@crates//:data.bzl` therefore
# does not carry it either, so a Bazel macro cannot derive the list. Reading the
# manifest from a test is the remaining option.

set -euo pipefail

manifest="$1"
shift

declared=$(printf '%s\n' "$@" | sort)

found=$(awk '
    function flush() {
        if (in_test && harness == "false" && name != "") {
            print name
        }
        in_test = 0
        name = ""
        harness = ""
    }
    function value(line) {
        sub(/^[^=]*=[[:space:]]*/, "", line)
        gsub(/"/, "", line)
        gsub(/[[:space:]]/, "", line)
        return line
    }
    /^[[:space:]]*\[/ { flush() }
    /^[[:space:]]*\[\[test\]\][[:space:]]*$/ { in_test = 1; next }
    in_test && /^[[:space:]]*name[[:space:]]*=/ { name = value($0) }
    in_test && /^[[:space:]]*harness[[:space:]]*=/ { harness = value($0) }
    END { flush() }
' "$manifest" | sort)

if [[ "$declared" == "$found" ]]; then
    exit 0
fi

echo "$manifest and its BUILD file disagree about which suites are harness = false." >&2
echo >&2
echo "  harness = false in Cargo.toml: ${found:-(none)}" >&2
echo "  no_harness in crate_tests:     ${declared:-(none)}" >&2
echo >&2
echo "Make the crate_tests call list exactly the stems the manifest declares." >&2
echo "A stem missing from no_harness is built with the libtest harness, and a" >&2
echo "suite whose only entry point is datatest_stable::harness! then runs zero" >&2
echo "cases while reporting success." >&2
exit 1
