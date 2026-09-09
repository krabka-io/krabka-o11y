#!/usr/bin/env bash
# Stages a datatest corpus as real files, then runs the suite over it.
#
# `datatest_stable` finds its cases by walking the corpus directory and keeping
# the entries whose type is a regular file. Every input Bazel hands a test is a
# symlink -- in the runfiles tree and again inside the sandbox -- so the walk
# yields nothing and the suite dies with "no test cases found" over a corpus
# that is sitting right there in its own runfiles. Copying the corpus with `-L`
# is what turns those symlinks into files the walk will accept.
#
# Argument 1 is the test binary, 2 the staged corpus directory, 3 the crate's
# workspace path -- the directory `cargo test` would have run the suite from,
# and so the one its corpus paths are written against. All three are passed by
# //bazel/defs.bzl; the corpus is laid out by //tools/datatest:defs.bzl.
set -euo pipefail

# `$(rootpath)` yields a path relative to the runfiles root, and a test does not
# run from there. Everything handed to this script is resolved against it.
runfile() {
    if [[ -e "$1" ]]; then
        printf '%s' "$1"
    else
        printf '%s' "${TEST_SRCDIR}/${TEST_WORKSPACE}/$1"
    fi
}

binary="$(realpath "$(runfile "$1")")"
corpus="$(runfile "$2")"
package="$3"

staging="${TEST_TMPDIR}/datatest"
rm -rf "${staging}"
mkdir -p "${staging}"
cp -RL "${corpus}/." "${staging}/"

# The crate's own directory need not hold any of the corpus -- one suite reads
# only a sibling crate's -- and Bazel drops an empty directory from a staged
# tree, so it is made here rather than there.
mkdir -p "${staging}/${package}"

# datatest-stable's own hook for running from somewhere other than the process's
# working directory. The suite chdirs here and reads its corpus by the relative
# path it was written with.
export __DATATEST_CWD="${staging}/${package}"

exec "${binary}" "${@:4}"
