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

# Bazel runs one test action per core by default, so a 32-shard sweep reaches
# the link step 32 ways at once. Each `ld.lld` holds 1.5-2.0 GB, which demands
# roughly 58 GB against this box's 31 GB and takes the whole VM down -- twice
# now, both times looking like an unexplained restart rather than an OOM.
# Shards still divide the work; only how many link at once is bounded.
readonly CONCURRENT_SHARDS=8

# Test scratch is left where Bazel puts it: under its own output base, which is
# on disk already. Pointing TMPDIR at a directory outside the sandbox instead
# breaks every test that calls tempfile::tempdir(), and cargo-mutants then
# refuses the whole shard because its unmutated baseline suite fails -- four of
# observability's thirty-two shards measured nothing that way, silently.
# Deliberately not under /tmp. A sweep runs for hours and this machine clears
# /tmp on restart -- two sweeps were lost that way, and a lost sweep is worse
# than a slow one because the totals it never wrote read as "nothing missed".
readonly LOG_DIR="${KRABKA_MUTANTS_LOG_DIR:-$HOME/krabka-work/sweep-results}"
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
    --local_test_jobs="$CONCURRENT_SHARDS" \
    --local_resources=memory=HOST_RAM*.6 \
    > "$log" 2>&1

  timed_out=$(grep -c 'Test timed out' "$log")
  # A shard can finish without reporting. cargo-mutants refuses to start when
  # an unmutated integration suite fails inside its sandbox -- which happens
  # even for a suite that passes under plain cargo and plain bazel -- and that
  # shard then measures nothing while the run still looks successful. Compare
  # what reported against what ran, because the totals below are summed only
  # from the shards that spoke.
  shards_run=$(grep -oE 'shard [0-9]+ of [0-9]+' "$log" | sort -u | wc -l)
  shards_reporting=$(grep -cE 'mutants: [0-9]+ caught' "$log")
  baseline_refused=$(grep -c 'does not pass; fix it first' "$log")
  printf '%-16s ' "$crate"
  grep -ohE '[0-9]+ mutants: [0-9]+ caught, [0-9]+ missed, [0-9]+ unviable' "$log" \
    | awk -v t="$timed_out" -v ran="$shards_run" -v spoke="$shards_reporting" -v refused="$baseline_refused" '
        {total += $1; caught += $3; missed += $5; unviable += $7}
        END {
          printf "total %-5d caught %-5d missed %-4d unviable %-4d shards %d",
                 total, caught, missed, unviable, NR
          if (t > 0) printf "  [%d shard(s) TIMED OUT]", t
          if (ran > spoke) printf "  [%d of %d shards SILENT -- totals cover only %d]", ran - spoke, ran, spoke
          if (refused > 0) printf "  [%d shard(s) refused: a baseline suite fails inside the sandbox]", refused
          printf "\n"
        }'
done
