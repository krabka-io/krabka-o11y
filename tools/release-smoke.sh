#!/usr/bin/env bash
set -euo pipefail

new_image=${1:?usage: release-smoke.sh NEW_IMAGE [OLD_IMAGE] [EVIDENCE_DIR]}
old_image=${2:-}
evidence_dir=${3:-qualification/evidence/release-smoke}
compose_file=deploy/compose/docker-compose.yaml
project="krabka-release-${GITHUB_RUN_ID:-$$}-${GITHUB_RUN_ATTEMPT:-1}"
tenant=release-smoke
base_marker="release-${GITHUB_SHA:-local}-$(date +%s)"
marker=${base_marker}
services=(
  metrics-distributor metrics-block-builder metrics-querier
  logs-distributor logs-block-builder logs-querier
  traces-distributor traces-block-builder traces-querier
  profiles-distributor profiles-block-builder profiles-querier
)

mkdir -p "${evidence_dir}"
compose=(docker compose --project-name "${project}" -f "${compose_file}")
cleanup() { "${compose[@]}" down --volumes --remove-orphans --timeout 5 > /dev/null 2>&1 || true; }
trap cleanup EXIT

image_id() {
  docker image inspect --format '{{.Id}}' "$1"
}

image_digest() {
  local image=$1 explicit=${2:-} digest manifest
  if [[ -n ${explicit} ]]; then
    digest=${explicit}
  elif [[ ${image} == *krabka-o11y:dev ]]; then
    manifest=$(bazel cquery -c opt //bazel/images/krabka:image_manifest_blob --output=files)
    digest="sha256:$(sha256sum "${manifest}" | cut -d ' ' -f 1)"
  elif [[ ${image} == *@sha256:* ]]; then
    digest=${image##*@}
  else
    digest=$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "${image}" |
      awk 'NR == 1 { sub(/^.*@/, ""); print; exit }')
  fi
  [[ ${digest} =~ ^sha256:[0-9a-f]{64}$ ]]
  printf '%s\n' "${digest}"
}

normalize_local_image() {
  local image_id
  if [[ ${new_image} == krabka-o11y:dev ]] && ! docker image inspect "${new_image}" >/dev/null 2>&1; then
    image_id=$(docker image ls --filter reference=krabka-o11y:dev --format '{{.ID}}' | sed -n '1p')
    test -n "${image_id}"
    new_image=localhost/krabka-o11y:dev
    docker tag "${image_id}" "${new_image}"
  fi
}

wait_for() {
  local name=$1 url=$2 body=${3:-}
  for _ in $(seq 1 90); do
    if [[ -n ${body} ]]; then
      response=$(curl -fsS "$url" -H "X-Scope-OrgID: ${tenant}" \
        -H 'Content-Type: application/json' --data "${body}" 2>/dev/null) || response=
    else
      response=$(curl -fsS "$url" -H "X-Scope-OrgID: ${tenant}" 2>/dev/null) || response=
    fi
    if grep -q "${marker}\|alloy" <<<"${response}"; then
      printf '%s\n' "${response}" >"${evidence_dir}/${name}.json"
      return 0
    fi
    sleep 2
  done
  echo "${name} query did not observe the smoke corpus" >&2
  return 1
}

wait_ready() {
  local url=$1
  for _ in $(seq 1 90); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "${url} did not become ready" >&2
  return 1
}

wait_ingest() {
  local url=$1
  for _ in $(seq 1 90); do
    if curl -sS -o /dev/null "${url}" 2>/dev/null; then
      return 0
    fi
    sleep 2
  done
  echo "${url} did not accept connections" >&2
  return 1
}

wait_stack_ready() {
  local url
  for url in \
    http://127.0.0.1:4041/ready \
    http://127.0.0.1:9090/ready \
    http://127.0.0.1:3100/ready \
    http://127.0.0.1:3101/ready \
    http://127.0.0.1:3201/ready \
    http://127.0.0.1:4040/ready \
    http://127.0.0.1:4042/ready; do
    wait_ready "${url}"
  done
  wait_ingest http://127.0.0.1:9999/
  wait_ingest http://127.0.0.1:14318/
}

send_corpus() {
  local now_ns now_s trace_id span_id
  now_ns=$(date +%s%N)
  now_s=$(date +%s)
  trace_id=$(printf '%s' "${marker}" | sha256sum | cut -c 1-32)
  span_id=${trace_id:0:16}
  curl -fsS http://127.0.0.1:9999/loki/api/v1/push \
    -H 'Content-Type: application/json' \
    --data "{\"streams\":[{\"stream\":{\"job\":\"${marker}\"},\"values\":[[\"${now_ns}\",\"${marker}\"]]}]}" >/dev/null
  curl -fsS http://127.0.0.1:14318/v1/traces \
    -H 'Content-Type: application/json' \
    --data "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"${marker}\"}}]},\"scopeSpans\":[{\"spans\":[{\"traceId\":\"${trace_id}\",\"spanId\":\"${span_id}\",\"name\":\"${marker}\",\"startTimeUnixNano\":\"${now_ns}\",\"endTimeUnixNano\":\"$((now_ns + 1000000))\"}]}]}]}" >/dev/null
  printf '%s\n' "${now_s}" >"${evidence_dir}/corpus-time"
}

query_corpus() {
  local stage=$1 now_s
  now_s=$(( $(date +%s) + 60 ))
  wait_for "${stage}-metrics" \
    'http://127.0.0.1:9090/api/v1/query?query=alloy_build_info'
  wait_for "${stage}-logs" \
    "http://127.0.0.1:3101/loki/api/v1/query_range?query=%7Bjob%3D%22${marker}%22%7D"
  wait_for "${stage}-traces" \
    "http://127.0.0.1:3201/api/search?q=%7Bresource.service.name%3D%22${marker}%22%7D&start=0&end=${now_s}"
  wait_for "${stage}-profiles" \
    'http://127.0.0.1:4042/querier.v1.QuerierService/Series' \
    '{"matchers":[],"labelNames":["service_name","__profile_type__"]}'
}

start_stack() {
  export KRABKA_O11Y_IMAGE=$1
  "${compose[@]}" up -d --wait --wait-timeout 300
  wait_stack_ready
}

normalize_local_image
if ! docker image inspect "${new_image}" >/dev/null 2>&1; then
  docker pull --platform linux/amd64 "${new_image}" >/dev/null
fi
new_id=$(image_id "${new_image}")
new_digest=$(image_digest "${new_image}" "${KRABKA_NEW_IMAGE_DIGEST:-}")
printf 'image=%s\ndigest=%s\nimage_id=%s\n' "${new_image}" "${new_digest}" "${new_id}" \
  >"${evidence_dir}/new-image.txt"

if [[ -n ${old_image} ]]; then
  docker pull --platform linux/amd64 "${old_image}" >/dev/null
  old_id=$(image_id "${old_image}")
  old_digest=$(image_digest "${old_image}")
  printf 'image=%s\ndigest=%s\nimage_id=%s\n' "${old_image}" "${old_digest}" "${old_id}" \
    >"${evidence_dir}/old-image.txt"
  start_stack "${old_image}"
  send_corpus
  query_corpus old
  export KRABKA_O11Y_IMAGE=${new_image}
  step=0
  for service in "${services[@]}"; do
    step=$((step + 1))
    "${compose[@]}" up -d --no-deps --force-recreate --wait --wait-timeout 300 "${service}"
    wait_stack_ready
    marker="${base_marker}-upgrade-${step}"
    send_corpus
    query_corpus "upgrade-${service}"
  done
  export KRABKA_O11Y_IMAGE=${old_image}
  step=0
  for service in "${services[@]}"; do
    step=$((step + 1))
    "${compose[@]}" up -d --no-deps --force-recreate --wait --wait-timeout 300 "${service}"
    wait_stack_ready
    marker="${base_marker}-rollback-${step}"
    send_corpus
    query_corpus "rollback-${service}"
  done
  marker=${base_marker}
  query_corpus rollback-pre-upgrade
else
  start_stack "${new_image}"
  send_corpus
  query_corpus clean
fi

"${compose[@]}" ps --format json >"${evidence_dir}/compose.json"
find "${evidence_dir}" -maxdepth 1 -type f ! -name SHA256SUMS -print0 |
  sort -z | xargs -0 sha256sum >"${evidence_dir}/SHA256SUMS"
