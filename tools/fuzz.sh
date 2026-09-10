#!/usr/bin/env bash
#
# Runs the fuzz targets for a bounded time.
#
#   tools/fuzz.sh                      # every target, 60 seconds each
#   tools/fuzz.sh 300                  # every target, 300 seconds each
#   tools/fuzz.sh 300 jaeger_compact_thrift zipkin_json
#
# The nightly `fuzz` job in //.github/workflows/ci.yml runs this script, so a
# local run and a CI run use the same flags and report the same way.
#
# Each target gets its own bound rather than one bound for the set, so a slow
# target cannot take the whole budget. The run is not open-ended: a fuzzer left
# running finds more, but a bounded run is what a scheduled job can report on,
# and it is what fits a shared machine.
#
# The corpus under //fuzz/corpus is seed input, not a working directory. This
# script copies it into a scratch directory and lets libFuzzer grow that copy,
# so a run never edits the checked-in seeds. Keep an input the run finds by
# copying it back by hand.
#
# The sanitizer is off. This workspace sets `unsafe_code = "forbid"`, so
# Address Sanitizer has no memory-unsafety to find, and what these targets look
# for is a panic, a hang, or an allocation sized from an attacker's number.
# libFuzzer reports all three on its own, and the build is about half the size
# without the sanitizer.
set -euo pipefail

cd "$(dirname "$0")/.."

seconds="${1:-60}"
shift || true

targets=("$@")
if [ "${#targets[@]}" -eq 0 ]; then
  mapfile -t targets < <(cargo fuzz list --fuzz-dir fuzz)
fi

scratch="$(mktemp -d)"
trap 'rm -rf "${scratch}"' EXIT

echo "building ${#targets[@]} targets"
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-$(nproc)}" \
  cargo +nightly fuzz build --fuzz-dir fuzz -s none --codegen-units 16

failed=()
for target in "${targets[@]}"; do
  work="${scratch}/${target}"
  mkdir -p "${work}"
  if [ -d "fuzz/corpus/${target}" ]; then
    cp -a "fuzz/corpus/${target}/." "${work}/"
  fi
  seeds="$(find "${work}" -type f | wc -l)"

  echo
  echo "=== ${target}: ${seconds}s, ${seeds} seeds ==="
  # `-timeout` bounds one input rather than the run, so a single slow decode is
  # reported as a hang instead of eating the target's whole budget.
  if ! cargo +nightly fuzz run --fuzz-dir fuzz -s none --codegen-units 16 \
    "${target}" "${work}" -- \
    -max_total_time="${seconds}" \
    -timeout=25 \
    -rss_limit_mb=2048 \
    -print_final_stats=1; then
    failed+=("${target}")
  fi
done

echo
if [ "${#failed[@]}" -ne 0 ]; then
  echo "::error::fuzz targets failed: ${failed[*]}"
  echo "libFuzzer wrote the input for each under fuzz/artifacts/<target>/"
  exit 1
fi
echo "all ${#targets[@]} targets ran ${seconds}s without a finding"
