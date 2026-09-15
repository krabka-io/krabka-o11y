#!/usr/bin/env bash
set -euo pipefail

tarball="$1"
image="$2"
binary="$3"
version="$4"
revision="${5:-}"

docker load --input "$tarball" >/dev/null
reported="$(docker run --rm --entrypoint "$binary" "$image" --version 2>&1)"

case "$reported" in
  *"version $version"* | *"version v$version"*) ;;
  *)
    printf '%s\n' "$reported" >&2
    echo "$image did not report version $version" >&2
    exit 1
    ;;
esac

if [[ -n "$revision" && "$reported" != *"${revision:0:7}"* ]]; then
  printf '%s\n' "$reported" >&2
  echo "$image did not report revision $revision" >&2
  exit 1
fi

test "$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image")" = linux/amd64
