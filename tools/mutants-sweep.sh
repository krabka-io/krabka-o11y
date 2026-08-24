#!/usr/bin/env bash
#
# Runs the cargo-mutants sweep over one or more crates and prints a one-line
# summary per crate.
#
#   tools/mutants-sweep.sh [crate ...]
#
# With no arguments every crate that defines a mutants target is swept.
#
# Two things about the harness are worth knowing before changing this script.
#
# A shard that misses its deadline is killed, and a killed shard writes no
# summary line at all rather than failing loudly. The totals below are summed
# from the lines the shards do write, so a crate whose shards all died reads
# as "total 0 ... missed 0" -- indistinguishable at a glance from a crate with
# nothing left to kill. The timed-out-shards count is printed for exactly that
# reason: trust a zero only when it is zero.
#
# `mutants_timeout = "eternal"` in the BUILD files is the largest category
# Bazel offers, but it is 3600s rather than unlimited, and the larger crates
# need longer. --test_timeout overrides the category and is the only way past
# an hour.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1

readonly TIMEOUT_SECONDS=36000
readonly LOG_DIR="${TMPDIR:-/tmp}/krabka-mutants"
mkdir -p "$LOG_DIR"

crates=("$@")
if [[ ${#crates[@]} -eq 0 ]]; then
  mapfile -t crates < <(
    grep -l 'cargo_mutants_test' crates/*/BUILD.bazel | sed 's|crates/||; s|/BUILD.bazel||'
  )
fi

printf 'sweeping %d crate(s), logs in %s\n' "${#crates[@]}" "$LOG_DIR"

for crate in "${crates[@]}"; do
  log="$LOG_DIR/$crate.log"
  bazel test "//crates/$crate:${crate}_mutants" \
    --nocache_test_results --test_output=all --test_timeout="$TIMEOUT_SECONDS" \
    > "$log" 2>&1

  timed_out=$(grep -c 'Test timed out' "$log")
  printf '%-16s ' "$crate"
  grep -ohE '[0-9]+ mutants: [0-9]+ caught, [0-9]+ missed, [0-9]+ unviable' "$log" \
    | awk -v t="$timed_out" '
        {total += $1; caught += $3; missed += $5; unviable += $7}
        END {
          printf "total %-5d caught %-5d missed %-4d unviable %-4d shards %d",
                 total, caught, missed, unviable, NR
          if (t > 0) printf "  [%d shard(s) TIMED OUT -- totals are incomplete]", t
          printf "\n"
        }'
done
