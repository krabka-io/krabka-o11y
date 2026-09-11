#!/usr/bin/env bash
# Checks that every role config in //deploy/roles names only flags its binary
# has.
#
# A config key that names no flag stops the binary at start-up. That failure
# belongs here, where a renamed flag fails a test, rather than in a rollout,
# where it fails as a pod that will not start.
#
# Each binary is run with its config file and one argument no binary has.
# `argv_with_config_file` reads and checks the file before clap parses the
# command line, so exactly one of two things happens:
#
#   the file was rejected  -- the message names the file and the offending key,
#                             and clap never ran
#   the file was accepted  -- clap ran and rejected the deliberate bad
#                             argument, naming it
#
# The test looks for clap's complaint about that argument. Exit status will not
# separate the two: `krabka-traces` returns 2 for a rejected config file, which
# is also clap's usage status.
#
# Neither case runs the role. Nothing binds a port and nothing dials a broker.
set -uo pipefail

metrics=$1
metrics_service=$2
observability=$3
profiles=$4
traces=$5
roles=$(dirname "$6")

failed=0

readonly BAD_ARGUMENT=--krabka-deploy-test-not-a-flag

check() {
  local binary=$1 role=$2 output status
  output=$("$binary" "--config.file=$roles/$role.yaml" "$BAD_ARGUMENT" 2>&1)
  status=$?
  if [[ $status -ne 0 && $output == *"$BAD_ARGUMENT"* ]]; then
    echo "ok       $role"
    return
  fi
  echo "NOT OK   $role: exit $status"
  echo "$output" | sed 's/^/         /'
  failed=1
}

check "$metrics" metrics-distributor
check "$metrics" metrics-block-builder
check "$metrics_service" metrics-querier
check "$observability" logs-distributor
check "$observability" logs-block-builder
check "$observability" logs-querier
check "$observability" logs-all
check "$traces" traces-distributor
check "$traces" traces-block-builder
check "$traces" traces-querier
check "$traces" traces-all
check "$profiles" profiles-distributor
check "$profiles" profiles-block-builder
check "$profiles" profiles-querier
check "$profiles" profiles-all

exit $failed
