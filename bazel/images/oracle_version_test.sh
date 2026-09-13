#!/usr/bin/env bash
set -euo pipefail

tarball="$1"
image="$2"
binary="$3"
version="$4"
revision="$5"

docker load --input "$tarball" >/dev/null
reported="$(docker run --rm --entrypoint "$binary" "$image" --version 2>&1)"

case "$reported" in
  *"version $version "* | *"version v$version "*) ;;
  *)
    printf '%s\n' "$reported" >&2
    echo "$image did not report version $version" >&2
    exit 1
    ;;
esac

case "$reported" in
  *"revision: ${revision:0:8}"*) ;;
  *)
    printf '%s\n' "$reported" >&2
    echo "$image did not report revision $revision" >&2
    exit 1
    ;;
esac

case "$reported" in
  *"platform:         linux/amd64"*) ;;
  *)
    printf '%s\n' "$reported" >&2
    echo "$image is not the linux/amd64 oracle" >&2
    exit 1
    ;;
esac
