#!/usr/bin/env bash
#
# Runs the Criterion benchmarks for a bounded time.
#
#   tools/bench.sh                      # every bench, the CI budget
#   tools/bench.sh --quick              # every bench, a few seconds each
#   tools/bench.sh --quick index_prune pprof_merge
#   tools/bench.sh index_prune
#
# The nightly `bench` job in //.github/workflows/ci.yml runs this script, so a
# local run and a CI run use the same flags and report the same way -- the same
# arrangement //tools/fuzz.sh has with the `fuzz` job.
#
# The budget is the point of this script. Criterion's defaults are 3 seconds
# of warm-up and 5 of measurement over 100 samples, per benchmark id, and //benches
# declares well over a hundred of them; left at the defaults this is hours.
# What the numbers below buy instead is enough samples for Criterion to report
# a confidence interval, which is what //tools/bench-ratchet.py reads to decide
# whether a measurement is steady enough to gate on at all. A tighter budget
# does not make the gate stricter, it makes the interval wider and the gate
# skip more often.
#
# `--quick` exists because a full benchmark suite takes far longer than a
# person expects on a shared machine. It is the mode to use when the question
# is "does this compile and produce a number", which is what a change to the
# benchmarks themselves asks.
#
# The run writes Criterion's own HTML under benches/target/criterion/report,
# which the CI job uploads whole. That is the artifact: the violin plots and
# the per-id pages are what a person reads after the ratchet has said which
# line moved.
set -euo pipefail

cd "$(dirname "$0")/.."

manifest="benches/Cargo.toml"

# The CI budget. Ten samples is Criterion's floor for the statistics it
# reports, and every group in //benches already asks for `sample_size(10)`
# where an iteration is milliseconds; naming it here as well covers the groups
# that do not, and makes one number the thing to change.
sample_size="${KRABKA_BENCH_SAMPLE_SIZE:-10}"
measurement_time="${KRABKA_BENCH_MEASUREMENT_TIME:-5}"
warm_up_time="${KRABKA_BENCH_WARM_UP_TIME:-1}"

if [ "${1:-}" = "--quick" ]; then
  shift
  sample_size=10
  measurement_time=1
  warm_up_time=0.5
fi

benches=("$@")
if [ "${#benches[@]}" -eq 0 ]; then
  # The `[[bench]]` names, read out of the manifest rather than repeated here.
  mapfile -t benches < <(
    awk '
      /^\[\[bench\]\]$/ { inside = 1; next }
      inside && /^name = "/ { gsub(/^name = "|"$/, ""); print; inside = 0 }
    ' "${manifest}"
  )
fi

if [ "${#benches[@]}" -eq 0 ]; then
  echo "::error::${manifest} declares no [[bench]] target" >&2
  exit 1
fi

# Debug info is the bulk of what a release build of the DataFusion pin writes
# and none of it is read here, the same reason the `cargo` job sets it.
export CARGO_PROFILE_BENCH_DEBUG="${CARGO_PROFILE_BENCH_DEBUG:-0}"

# One build for the whole set, so a per-bench failure below is a benchmark
# failing rather than a compile error reported once per target.
echo "building ${#benches[@]} benchmarks"
build=()
for bench in "${benches[@]}"; do
  build+=(--bench "${bench}")
done
cargo bench --manifest-path "${manifest}" --locked "${build[@]}" --no-run

failed=()
for bench in "${benches[@]}"; do
  echo
  echo "=== ${bench}: ${sample_size} samples, ${measurement_time}s each ==="
  if ! cargo bench --manifest-path "${manifest}" --locked --bench "${bench}" -- \
    --sample-size "${sample_size}" \
    --measurement-time "${measurement_time}" \
    --warm-up-time "${warm_up_time}"; then
    failed+=("${bench}")
  fi
done

echo
if [ "${#failed[@]}" -ne 0 ]; then
  echo "::error::benchmarks failed to run: ${failed[*]}"
  echo "a benchmark that fails here panicked; it did not merely get slower"
  exit 1
fi
echo "all ${#benches[@]} benchmarks ran"
echo "estimates under benches/target/criterion, report at"
echo "  benches/target/criterion/report/index.html"
echo
echo "the verdict is tools/bench-ratchet.py, which reads those estimates"
