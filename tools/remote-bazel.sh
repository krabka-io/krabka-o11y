#!/usr/bin/env bash
# Run a Bazel build/test command on a BuildBuddy executor. Whole Bazel runners
# preserve the existing toolchain's sandbox paths; per-action RBE currently
# trips rules_rs's check for absolute working directories in compiled crates.
set -euo pipefail

if [[ $# -eq 0 || ( "$1" != build && "$1" != test ) ]]; then
  echo "usage: tools/remote-bazel.sh <build|test> [Bazel arguments...]" >&2
  exit 2
fi

if [[ -z "${BUILDBUDDY_API_KEY:-}" ]]; then
  if [[ "${BUILDBUDDY_REQUIRED:-false}" == true ]]; then
    echo "::error::BUILDBUDDY_API_KEY is missing. Grant this repository access to the organization-level Actions secret."
    exit 1
  fi
  echo "No BuildBuddy key on this run; building locally."
  exec bazel "$@"
fi

if [[ "${BUILDBUDDY_API_KEY}" == *$'\n'* || "${BUILDBUDDY_API_KEY}" == *$'\r'* || "${BUILDBUDDY_API_KEY}" == -----BEGIN* ]]; then
  echo "::error::BUILDBUDDY_API_KEY must be a single-line API key."
  exit 1
fi

# A separate invocation per phase keeps the build, lint and test budgets below
# BuildBuddy's one-hour free-tier limit. The CLI authenticates from the inherited
# environment; the API key is never included in arguments or committed config.
exec bb remote \
  --os=linux \
  --arch=amd64 \
  --container_image=docker://docker.io/library/rust@sha256:0e2bcaef56d041a486784e54104a81aebe0da44bd03019bd70bc0401e42e4a97 \
  --env=PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  --env=CI=true \
  --run_from_commit="$(git rev-parse HEAD)" \
  --timeout=55m \
  --disable_retry \
  --runner_exec_properties=dockerUser=root \
  --runner_exec_properties=EstimatedCPU=16 \
  --runner_exec_properties=EstimatedMemory=64GB \
  --runner_exec_properties=EstimatedFreeDiskBytes=200GB \
  "$@" --remote_executor= --jobs=16
