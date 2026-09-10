#!/usr/bin/env bash
#
# Checks that every sibling workspace repeats //Cargo.toml's sibling pins
# exactly.
#
#   tools/check-fuzz-patch.sh
#
# //fuzz and //benches are workspaces of their own -- cargo-fuzz needs a
# nightly toolchain the rest of the repository does not use, and Criterion is a
# dev-dependency that must not reach the builds gating a pull request. Cargo
# reads `[patch.crates-io]` only from the workspace root it is building, so the
# sibling revisions in //Cargo.toml do not reach either of them and are
# repeated in both.
#
# Copies of a pin drift. //.github/workflows/sync-siblings.yml bumps the table
# in //Cargo.toml and knows nothing about the others, so a bump leaves those
# workspaces on the old revision. The fuzz targets then decode with one copy of
# the sibling crates while every other build uses another, and the benchmarks
# measure a different one again -- and nothing says so: every workspace
# resolves, every one builds, and the scheduled jobs keep reporting on code
# that is no longer what ships.
#
# This script compares the tables line by line. It reads no network and no
# checkout, so it costs about a second and runs on every pull request.
set -euo pipefail

cd "$(dirname "$0")/.."

root_manifest="Cargo.toml"
# Every workspace that repeats the table. A new one is added here and nowhere
# else.
sibling_manifests=(fuzz/Cargo.toml benches/Cargo.toml)

# Everything from the `[patch.crates-io]` header to the next table header or to
# the end of the file, with comments and blank lines dropped. Both manifests end
# with this table, so "the next table header" is a guard rather than a case
# either file exercises today.
patch_table() {
  awk '
    /^\[patch\.crates-io\]$/ { inside = 1; next }
    inside && /^\[/ { inside = 0 }
    inside && !/^[[:space:]]*(#|$)/ { print }
  ' "$1"
}

root_table="$(patch_table "${root_manifest}")"

if [ -z "${root_table}" ]; then
  echo "::error::${root_manifest} has no [patch.crates-io] table" >&2
  exit 1
fi

drifted=0
for manifest in "${sibling_manifests[@]}"; do
  sibling_table="$(patch_table "${manifest}")"

  if [ -z "${sibling_table}" ]; then
    echo "::error::${manifest} has no [patch.crates-io] table" >&2
    echo "copy the table from ${root_manifest}" >&2
    drifted=1
    continue
  fi

  if [ "${root_table}" = "${sibling_table}" ]; then
    echo "${manifest} matches ${root_manifest}: $(wc -l <<<"${root_table}") pins"
    continue
  fi

  echo "::error::${manifest} and ${root_manifest} disagree on [patch.crates-io]" >&2
  echo "left is ${root_manifest}, right is ${manifest}:" >&2
  diff <(printf '%s\n' "${root_table}") <(printf '%s\n' "${sibling_table}") >&2 || true
  echo >&2
  echo "copy the table from ${root_manifest} into ${manifest}, then run" >&2
  echo "  cargo generate-lockfile --manifest-path ${manifest}" >&2
  drifted=1
done

exit "${drifted}"
