#!/usr/bin/env bash
# Loads the Kafka images a container-driven suite needs, then runs it.
#
# The suites start containers through testcontainers, which by default pulls a
# missing image from the network mid-test. Loading Bazel's digest-pinned
# tarballs first means the tag the suite asks for is already present, so nothing
# is fetched while the test runs and the bytes are the ones the build pinned.
#
# Argument 1 is the test binary; KRABKA_IMAGE_TARS is a colon-separated list of
# tarballs, both passed as runfiles by //bazel:defs.bzl.
set -euo pipefail

if ! docker info >/dev/null 2>&1; then
    echo "docker_test.sh: no reachable Docker daemon." >&2
    echo "  These suites drive real Kafka containers. Start Docker, or run" >&2
    echo "  the default \`bazel test\`, which filters them out." >&2
    exit 1
fi

# `$(rootpath)` yields a path relative to the runfiles root, and a test does not
# run from there. Everything handed to this script is resolved against it.
runfile() {
    if [[ -e "$1" ]]; then
        printf '%s' "$1"
    elif [[ "$1" == external/* ]]; then
        # A path in another repository: those sit directly under the runfiles
        # root, keyed by repository name, not under this workspace's directory.
        printf '%s' "${TEST_SRCDIR}/${1#external/}"
    else
        printf '%s' "${TEST_SRCDIR}/${TEST_WORKSPACE}/$1"
    fi
}

binary="$(runfile "$1")"
shift

if [[ -n "${KRABKA_IMAGE_TARS:-}" ]]; then
    while IFS= read -r -d ':' tar || [[ -n "${tar}" ]]; do
        [[ -n "${tar}" ]] || continue
        # `docker load` is a no-op when the image is already present, so this
        # costs nothing on a warm daemon and is the whole fetch on a cold one.
        docker load --quiet --input "$(runfile "${tar}")" >/dev/null
    done <<<"${KRABKA_IMAGE_TARS}"
fi

# `--ignored`: under Cargo these cases are `#[ignore]`d, because they need the
# daemon this script just checked for. This is the target that runs them.
#
# `--test-threads=1`: a suite's tests share one port per process, so they still
# run one at a time *within* a binary. Different binaries no longer collide --
# each allocates its own -- which is why the targets themselves can now overlap.
# Per-test ports would lift this too; `jvm_acceptance` threads its address
# through 182 references, so that is a larger change than this one.
exec "${binary}" --ignored --test-threads=1 "$@"
