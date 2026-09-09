#!/usr/bin/env bash
#
# Checks the two hand-written sibling-crate lists against the siblings.
#
#   tools/check-sibling-members.sh
#
# Two lists name the crates that the sibling repositories hold, and both must
# stay complete:
#
#   * `[patch.crates-io]` in Cargo.toml points each sibling crate at a pinned
#     revision instead of at the registry. Cargo ignores a dependency's own
#     patch table, so a sibling crate that this repo reaches only through
#     another sibling crate needs an entry here as well.
#   * `SIBLING_MEMBERS` in MODULE.bazel gives each sibling crate its directory,
#     because rules_rs cannot match a crate name against the `crates/*` glob in
#     the sibling workspace manifest.
#
# Nothing else enforces that. sync-siblings.yml moves a revision and
# regenerates the lockfile, and it never adds a list entry. A sibling that adds
# a crate this repo then reaches gets that crate from crates.io. A sibling that
# moves a crate directory breaks Bazel analysis with a path error. This script
# fails the sync job in both cases, before the job opens a green pull request.
#
# The sibling workspaces are the source of truth. Cargo checked them out at the
# pinned revisions already, so the check reads them from disk and needs no
# network. Run it after `cargo generate-lockfile`, when the lockfile names the
# revision the lists must match.
set -euo pipefail

cd "$(dirname "$0")/.."

errors=()

fail() {
  errors+=("$1")
}

# --- The list in Cargo.toml -------------------------------------------------

declare -A patch_url=() patch_rev=()
in_patch_table=0
while IFS= read -r line; do
  if [[ $line == "[patch.crates-io]" ]]; then
    in_patch_table=1
    continue
  fi
  # Any other table header ends the patch table.
  if [[ $line == "["* ]]; then
    in_patch_table=0
  fi
  if ((in_patch_table)) &&
    [[ $line =~ ^([A-Za-z0-9_-]+)[[:space:]]*=[[:space:]]*\{[[:space:]]*git[[:space:]]*=[[:space:]]*\"([^\"]+)\"[[:space:]]*,[[:space:]]*rev[[:space:]]*=[[:space:]]*\"([0-9a-f]{40})\" ]]; then
    patch_url["${BASH_REMATCH[1]}"]="${BASH_REMATCH[2]}"
    patch_rev["${BASH_REMATCH[1]}"]="${BASH_REMATCH[3]}"
  fi
done <Cargo.toml

# A parser that matches nothing reads as a clean check, which is the one
# failure mode this script must not have.
if ((${#patch_url[@]} == 0)); then
  echo "check-sibling-members: found no git entries in [patch.crates-io] in Cargo.toml" >&2
  echo "check-sibling-members: the table moved or changed shape, and this parser needs the same change" >&2
  exit 1
fi

# --- The list in MODULE.bazel -----------------------------------------------

# `bazel_source` records which construct names the crate, so a failure can tell
# the reader where to edit.
declare -A bazel_dir=() bazel_source=()
in_dict=0
while IFS= read -r line; do
  if [[ $line == "SIBLING_MEMBERS = {" ]]; then
    in_dict=1
    continue
  fi
  if ((in_dict)) && [[ $line == "}" ]]; then
    in_dict=0
    continue
  fi
  if ((in_dict)) && [[ $line =~ \"([^\"]+)\":[[:space:]]*\"([^\"]+)\" ]]; then
    bazel_dir["${BASH_REMATCH[1]}"]="${BASH_REMATCH[2]}"
    bazel_source["${BASH_REMATCH[1]}"]="SIBLING_MEMBERS in MODULE.bazel"
  fi
done <MODULE.bazel

if ((${#bazel_dir[@]} == 0)); then
  echo "check-sibling-members: found no entries in SIBLING_MEMBERS in MODULE.bazel" >&2
  echo "check-sibling-members: the dict moved or changed shape, and this parser needs the same change" >&2
  exit 1
fi

# krabka-protocol is deliberately absent from SIBLING_MEMBERS: it needs a second
# annotation for `gen_build_script`, so it carries its own `crate.annotation`
# call with the same `strip_prefix`. Read every top-level annotation that sets
# `strip_prefix` to a literal, so that crate counts as covered and so a second
# one-off annotation later needs no change here. The `crate.annotation` calls
# that the SIBLING_MEMBERS comprehension generates are indented and pass
# variables rather than literals, so neither the anchor nor the quote test
# matches them.
while IFS=$'\t' read -r crate_name directory; do
  bazel_dir["$crate_name"]="$directory"
  bazel_source["$crate_name"]="the crate.annotation call for $crate_name in MODULE.bazel"
done < <(awk '
  /^crate\.annotation\(/ { inside = 1; crate = ""; prefix = ""; next }
  inside && /^\)/ {
    if (crate != "" && prefix != "") { print crate "\t" prefix }
    inside = 0
    next
  }
  inside && $1 == "crate" && $2 == "=" && $3 ~ /^"/ { crate = $3; gsub(/[",]/, "", crate) }
  inside && $1 == "strip_prefix" && $2 == "=" && $3 ~ /^"/ { prefix = $3; gsub(/[",]/, "", prefix) }
' MODULE.bazel)

# --- The siblings themselves ------------------------------------------------

# `--locked` keeps the check read-only. A stale lockfile fails here with
# cargo's own message rather than being rewritten under the caller.
metadata="$(cargo metadata --format-version 1 --locked)"

# The siblings share one crates.io namespace, so a crate name identifies one
# crate in one sibling. That makes a flat map by name enough.
declare -A member_dir=() member_repo=()
declare -A repo_seen=()
for crate_name in "${!patch_url[@]}"; do
  url="${patch_url[$crate_name]}"
  rev="${patch_rev[$crate_name]}"
  if [[ -n ${repo_seen["$url $rev"]:-} ]]; then
    continue
  fi
  repo_seen["$url $rev"]=1

  # Any one crate from the sibling locates the checkout, and `--no-deps` on it
  # then reports every member of that sibling workspace, including the members
  # this repo does not use.
  manifest="$(jq -r --arg source "git+$url?rev=$rev#$rev" \
    '[.packages[] | select(.source == $source) | .manifest_path][0] // empty' <<<"$metadata")"
  if [[ -z $manifest ]]; then
    fail "no crate in the resolved graph comes from $url at ${rev:0:7}, so this check cannot read that sibling. Every [patch.crates-io] entry for it is unused: drop the entries, or restore the dependency that used them."
    continue
  fi

  while IFS=$'\t' read -r name directory; do
    member_dir["$name"]="$directory"
    member_repo["$name"]="$url"
  done < <(cargo metadata --no-deps --format-version 1 --manifest-path "$manifest" |
    jq -r '.workspace_root as $root
           | .packages[]
           | [.name, (.manifest_path | ltrimstr($root + "/") | rtrimstr("/Cargo.toml"))]
           | @tsv')
done

# --- The checks -------------------------------------------------------------

for crate_name in "${!patch_url[@]}"; do
  url="${patch_url[$crate_name]}"

  if [[ -z ${bazel_dir["$crate_name"]:-} ]]; then
    fail "$crate_name is in [patch.crates-io] in Cargo.toml but in no list in MODULE.bazel. Add it to SIBLING_MEMBERS with its directory in the sibling, or Bazel resolves it against the sibling's crates/* glob and fails analysis."
    continue
  fi

  # A crate the pinned sibling no longer holds was renamed, removed, or moved
  # to another sibling. Cargo refuses a patch entry that its git repository
  # does not contain, so it reports this first. The branch stays as a backstop
  # that names both lists rather than Cargo.toml alone.
  if [[ ${member_repo["$crate_name"]:-} != "$url" ]]; then
    if [[ -n ${member_repo["$crate_name"]:-} ]]; then
      fail "$crate_name is pinned to $url in [patch.crates-io] in Cargo.toml, but it now lives in ${member_repo["$crate_name"]}. Point the entry at that sibling, and correct ${bazel_source["$crate_name"]}."
    else
      fail "$crate_name is in [patch.crates-io] in Cargo.toml and in ${bazel_source["$crate_name"]}, but $url no longer holds a crate with that name at the pinned revision. It was renamed or removed: drop both entries, or replace them with the new name."
    fi
    continue
  fi

  if [[ ${bazel_dir["$crate_name"]} != "${member_dir["$crate_name"]}" ]]; then
    fail "$crate_name moved to ${member_dir["$crate_name"]} in the sibling, and ${bazel_source["$crate_name"]} still says ${bazel_dir["$crate_name"]}. Bazel analysis fails on the old path: correct the directory."
  fi
done

for crate_name in "${!bazel_dir[@]}"; do
  if [[ -n ${patch_url["$crate_name"]:-} ]]; then
    continue
  fi
  # Every name in SIBLING_MEMBERS is a sibling crate, so an unpatched one is
  # drift either way. A standalone crate.annotation can carry a strip_prefix
  # for a git crate that is not a sibling, so judge that one only when a
  # sibling holds the name.
  if [[ ${bazel_source["$crate_name"]} == "SIBLING_MEMBERS in MODULE.bazel" ]] ||
    [[ -n ${member_dir["$crate_name"]:-} ]]; then
    fail "$crate_name is in ${bazel_source["$crate_name"]} but not in [patch.crates-io] in Cargo.toml. Cargo then takes it from crates.io while Bazel takes it from the sibling. Add the patch entry, or drop this entry if the sibling no longer holds the crate."
  fi
done

# A sibling crate that this repo reaches without a patch entry resolves from
# crates.io. That needs the name to exist there under another owner, which is
# why cargo stays quiet about it: it found a package with the right name.
while IFS= read -r crate_name; do
  if [[ -n ${member_repo["$crate_name"]:-} ]]; then
    fail "$crate_name comes from crates.io, and ${member_repo["$crate_name"]} holds a crate with that name at the pinned revision. The build links the registry copy, not the sibling: add it to [patch.crates-io] in Cargo.toml and to SIBLING_MEMBERS in MODULE.bazel."
  fi
done < <(jq -r '.packages[]
                | select(.source != null)
                | select(.source | startswith("git+") | not)
                | .name' <<<"$metadata")

# --- The verdict ------------------------------------------------------------

if ((${#errors[@]} > 0)); then
  echo "check-sibling-members: the sibling crate lists do not match the pinned revisions" >&2
  for error in "${errors[@]}"; do
    echo >&2
    echo "  ${error}" >&2
  done
  echo >&2
  exit 1
fi

echo "check-sibling-members: ${#patch_url[@]} sibling crates, both lists complete"
