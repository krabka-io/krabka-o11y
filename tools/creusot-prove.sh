#!/bin/bash
set -euo pipefail

expected="$(tr -d 'v\n' <.creusot-version)"
actual="$(cargo creusot version | sed -n 's/^cargo-creusot //p')"
if [[ "${actual}" != "${expected}" ]]; then
  echo "cargo-creusot ${actual:-missing} does not match v${expected}" >&2
  exit 1
fi

proof_target="$(mktemp -d /tmp/krabka-o11y-creusot.XXXXXX)"
trap 'rm -rf -- "${proof_target}"' EXIT
CARGO_TARGET_DIR="${proof_target}" cargo creusot \
  --package krabka-o11y-verified \
  --replay \
  --span-mode off
