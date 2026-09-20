#!/usr/bin/env bash
# Runs one disjoint part of a cargo-mutants target's Bazel shards directly.
# The aggregate ratchet must run after every part has uploaded its shard logs.
set -euo pipefail

cd "$(dirname "$0")/.." || exit 1

shards_for_part() {
  local total=$1 part=$2 parts=$3 shard
  for ((shard = part; shard < total; shard += parts)); do
    printf '%s\n' "$shard"
  done
}

valid_partition() {
  [[ $1 =~ ^[1-9][0-9]*$ ]] &&
    [[ $2 =~ ^[0-9]+$ ]] &&
    [[ $3 =~ ^[1-9][0-9]*$ ]] &&
    ((10#$2 < 10#$3))
}

self_test() {
  [[ $(shards_for_part 8 1 3 | paste -sd ' ') == "1 4 7" ]]
  valid_partition 8 0 2
  ! valid_partition 8 2 2
  ! valid_partition 0 0 2
  echo "mutants-part-sweep self-test passed"
}

if [[ ${1:-} == --self-test ]]; then
  self_test
  exit
fi

if [[ $# -ne 3 ]] || ! valid_partition 1 "$2" "$3"; then
  echo "usage: $0 <crate> <zero-based-part> <part-count>" >&2
  exit 2
fi

readonly crate=$1 part=$((10#$2)) parts=$((10#$3))
readonly target="//crates/$crate:${crate}_mutants"
shard_count=$(bazel query "$target" --output=xml |
  sed -n 's/.*<int name="shard_count" value="\([0-9][0-9]*\)"\/>.*/\1/p')
if ! [[ $shard_count =~ ^[1-9][0-9]*$ ]]; then
  echo "$target has no positive shard_count" >&2
  exit 2
fi
readonly shard_count
if ((part >= shard_count)); then
  echo "part $part selects no shard from $shard_count" >&2
  exit 2
fi

detected_cores=$(nproc)
if ((detected_cores > 4)); then
  detected_cores=4
fi
readonly concurrent="${KRABKA_MUTANTS_CONCURRENT_SHARDS:-$detected_cores}"
if ! [[ $concurrent =~ ^[1-9][0-9]*$ ]]; then
  echo "KRABKA_MUTANTS_CONCURRENT_SHARDS must be a positive integer" >&2
  exit 2
fi

readonly log_dir="${KRABKA_MUTANTS_LOG_DIR:-$HOME/krabka-work/sweep-results}"
readonly scratch_root="${KRABKA_MUTANTS_SCRATCH_DIR:-$(bazel info output_base)/mutants-parts}"
mkdir -p "$log_dir" "$scratch_root/$crate/part-$part"

bazel build "$target"
readonly runner="$(realpath "bazel-bin/crates/$crate/${crate}_mutants")"
readonly runfiles="$(realpath "bazel-bin/crates/$crate/${crate}_mutants.runfiles")"
started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
started_seconds=$SECONDS

run_shard() {
  local shard=$1
  local scratch="$scratch_root/$crate/part-$part/shard-$shard"
  local output="bazel-testlogs/crates/$crate/${crate}_mutants/shard_$((shard + 1))_of_$shard_count/test.log"
  mkdir -p "$scratch" "$(dirname "$output")"
  TEST_SRCDIR="$runfiles" RUNFILES_DIR="$runfiles" TEST_WORKSPACE=_main \
    TEST_TOTAL_SHARDS="$shard_count" TEST_SHARD_INDEX="$shard" \
    TEST_TMPDIR="$scratch" "$runner" >"$output" 2>&1 || true
}

mapfile -t selected_shards < <(shards_for_part "$shard_count" "$part" "$parts")
printf 'sweeping %s part %d of %d: shards %s\n' \
  "$crate" "$((part + 1))" "$parts" "${selected_shards[*]}"

pids=()
for shard in "${selected_shards[@]}"; do
  run_shard "$shard" &
  pids+=("$!")
  if ((${#pids[@]} == concurrent)); then
    for pid in "${pids[@]}"; do wait "$pid" || true; done
    pids=()
  fi
done
for pid in "${pids[@]}"; do wait "$pid" || true; done

metadata="$log_dir/$crate-part-$part.metadata.txt"
{
  printf 'commit=%s\n' "$(git rev-parse HEAD)"
  printf 'started_at=%s\n' "$started_at"
  printf 'duration_seconds=%d\n' "$((SECONDS - started_seconds))"
  printf 'part=%d\nparts=%d\nshard_count=%d\n' "$part" "$parts" "$shard_count"
  printf 'selected_shards=%s\n' "${selected_shards[*]}"
  printf 'command=%s %s %d %d\n' "$0" "$crate" "$part" "$parts"
  printf 'host=%s\n' "$(uname -a)"
  printf 'cpu_count=%s\n' "$(nproc)"
  printf 'memory_kib=%s\n' "$(awk '/^MemTotal:/ {print $2}' /proc/meminfo)"
  printf 'rustc=%s\n' "$(rustc --version --verbose | tr '\n' ';')"
  printf 'bazel=%s\n' "$(bazel version 2>/dev/null | tr '\n' ';')"
} >"$metadata"

checksums="$log_dir/$crate-part-$part.SHA256SUMS"
sha256sum "$metadata" >"$checksums"
for shard in "${selected_shards[@]}"; do
  log="bazel-testlogs/crates/$crate/${crate}_mutants/shard_$((shard + 1))_of_$shard_count/test.log"
  sha256sum "$log" >>"$checksums"
  cat "$log"
done
