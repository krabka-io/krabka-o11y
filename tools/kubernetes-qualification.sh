#!/usr/bin/env bash
set -euo pipefail

new_image=${1:?usage: kubernetes-qualification.sh NEW_IMAGE OLD_IMAGE [EVIDENCE_DIR]}
old_image=${2:?usage: kubernetes-qualification.sh NEW_IMAGE OLD_IMAGE [EVIDENCE_DIR]}
evidence_dir=${3:-qualification/evidence/kubernetes}
cluster="krabka-m17-${GITHUB_RUN_ID:-$$}-${GITHUB_RUN_ATTEMPT:-1}"
namespace=krabka-o11y
tenant=release-smoke
base_marker="kubernetes-${GITHUB_SHA:-local}-$(date +%s)"
marker=${base_marker}
mkdir -p "${evidence_dir}"
commands="${evidence_dir}/commands.log"
: >"${commands}"

run() {
  printf '+ ' >>"${commands}"
  printf '%q ' "$@" >>"${commands}"
  printf '\n' >>"${commands}"
  "$@"
}

wait_deployments() {
  local deployment
  while IFS= read -r deployment; do
    run kubectl -n "${namespace}" rollout status "${deployment}" --timeout=15m
  done < <(kubectl -n "${namespace}" get deployments -l app.kubernetes.io/part-of=krabka-o11y -o name | sort)
}

forward_pids=()
cleanup() {
  if ((${#forward_pids[@]})); then kill "${forward_pids[@]}" 2>/dev/null || true; fi
  kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [[ ${new_image} == krabka-o11y:dev ]] && ! docker image inspect "${new_image}" >/dev/null 2>&1; then
  image_id=$(docker image ls --filter reference=krabka-o11y:dev --format '{{.ID}}' | sed -n '1p')
  test -n "${image_id}"
  new_image=localhost/krabka-o11y:dev
  run docker tag "${image_id}" "${new_image}"
fi

for image in "${new_image}" "${old_image}"; do
  if ! docker image inspect "${image}" >/dev/null 2>&1; then
    run docker pull --platform linux/amd64 "${image}"
  fi
done

run kind create cluster --name "${cluster}" --wait 120s
run kind load docker-image --name "${cluster}" "${new_image}"
run kind load docker-image --name "${cluster}" "${old_image}"
kubectl version -o yaml >"${evidence_dir}/cluster-version.yaml"
kubectl get nodes -o wide >"${evidence_dir}/nodes.txt"

kubectl kustomize deploy >"${evidence_dir}/manifests.current.yaml"
kubectl kustomize deploy >"${evidence_dir}/manifests.second.yaml"
run cmp "${evidence_dir}/manifests.current.yaml" "${evidence_dir}/manifests.second.yaml"
sed "s#image: ghcr.io/krabka-io/krabka-o11y:latest#image: ${old_image}#" \
  <"${evidence_dir}/manifests.current.yaml" >"${evidence_dir}/manifests.yaml"
run kubectl apply --server-side -f "${evidence_dir}/manifests.yaml"
run kubectl -n "${namespace}" create configmap qualification-alloy \
  --from-file=config.alloy=deploy/compose/alloy-config.alloy
run kubectl apply -f deploy/kubernetes/qualification-client.yaml
run kubectl -n "${namespace}" wait --for=condition=complete job/minio-buckets --timeout=10m
run kubectl -n "${namespace}" rollout status statefulset/broker --timeout=10m
run kubectl -n "${namespace}" rollout status statefulset/minio --timeout=10m
wait_deployments

port_forward() {
  local target=$1 ports=$2
  kubectl -n "${namespace}" port-forward "${target}" "${ports}" \
    >>"${evidence_dir}/port-forward.log" 2>&1 &
  forward_pids+=("$!")
}

wait_http() {
  local url=$1
  for _ in $(seq 1 90); do
    curl -fsS "${url}" >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "${url} did not become available" >&2
  return 1
}
wait_reachable() {
  local url=$1
  for _ in $(seq 1 90); do
    curl -sS -o /dev/null "${url}" 2>/dev/null && return 0
    sleep 2
  done
  echo "${url} did not become reachable" >&2
  return 1
}
start_forwards() {
  if ((${#forward_pids[@]})); then
    kill "${forward_pids[@]}" 2>/dev/null || true
    wait "${forward_pids[@]}" 2>/dev/null || true
  fi
  forward_pids=()
  port_forward service/alloy 19999:9999
  port_forward service/alloy 14318:4318
  port_forward service/metrics-query-frontend 19090:9090
  port_forward service/logs-querier 13101:3100
  port_forward service/traces-query-frontend 13201:3200
  port_forward service/profiles-query-frontend 14042:4040
  wait_reachable http://127.0.0.1:19999/
  wait_reachable http://127.0.0.1:14318/
  wait_http http://127.0.0.1:19090/ready
  wait_http http://127.0.0.1:13101/ready
  wait_http http://127.0.0.1:13201/ready
  wait_http http://127.0.0.1:14042/ready
}
ensure_forwards() {
  local pid
  for pid in "${forward_pids[@]}"; do
    kill -0 "${pid}" 2>/dev/null || { start_forwards; return; }
  done
}
start_forwards

send_corpus() {
  local now_ns trace_id span_id
  now_ns=$(date +%s%N)
  trace_id=$(printf '%s' "${marker}" | sha256sum | cut -c 1-32)
  span_id=${trace_id:0:16}
  run curl -fsS http://127.0.0.1:19999/loki/api/v1/push \
    -H 'Content-Type: application/json' \
    --data "{\"streams\":[{\"stream\":{\"job\":\"${marker}\"},\"values\":[[\"${now_ns}\",\"${marker}\"]]}]}"
  run curl -fsS http://127.0.0.1:14318/v1/traces \
    -H 'Content-Type: application/json' \
    --data "{\"resourceSpans\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"${marker}\"}}]},\"scopeSpans\":[{\"spans\":[{\"traceId\":\"${trace_id}\",\"spanId\":\"${span_id}\",\"name\":\"${marker}\",\"startTimeUnixNano\":\"${now_ns}\",\"endTimeUnixNano\":\"$((now_ns + 1000000))\"}]}]}]}"
}

wait_for() {
  local name=$1 url=$2 body=${3:-} response
  for _ in $(seq 1 120); do
    ensure_forwards
    if [[ -n ${body} ]]; then
      response=$(curl -fsS "${url}" -H "X-Scope-OrgID: ${tenant}" \
        -H 'Content-Type: application/json' --data "${body}" 2>/dev/null) || response=
    else
      response=$(curl -fsS "${url}" -H "X-Scope-OrgID: ${tenant}" 2>/dev/null) || response=
    fi
    if grep -q "${marker}\|alloy" <<<"${response}"; then
      printf '%s\n' "${response}" >"${evidence_dir}/${name}.json"
      return 0
    fi
    sleep 2
  done
  echo "${name} did not observe the qualification corpus" >&2
  return 1
}

assert_absent() {
  local name=$1 url=$2 needle=$3 body=${4:-} response
  ensure_forwards
  if [[ -n ${body} ]]; then
    response=$(curl -fsS "${url}" -H 'X-Scope-OrgID: release-smoke-isolated' \
      -H 'Content-Type: application/json' --data "${body}")
  else
    response=$(curl -fsS "${url}" -H 'X-Scope-OrgID: release-smoke-isolated')
  fi
  printf '%s\n' "${response}" >"${evidence_dir}/${name}.json"
  if grep -q "${needle}" <<<"${response}"; then
    echo "${name} leaked another tenant's corpus" >&2
    return 1
  fi
}

query_corpus() {
  local stage=$1 now_s
  ensure_forwards
  now_s=$(( $(date +%s) + 60 ))
  wait_for "${stage}-metrics" 'http://127.0.0.1:19090/api/v1/query?query=alloy_build_info'
  wait_for "${stage}-logs" "http://127.0.0.1:13101/loki/api/v1/query_range?query=%7Bjob%3D%22${marker}%22%7D"
  wait_for "${stage}-traces" "http://127.0.0.1:13201/api/search?q=%7Bresource.service.name%3D%22${marker}%22%7D&start=0&end=${now_s}"
  wait_for "${stage}-profiles" 'http://127.0.0.1:14042/querier.v1.QuerierService/Series' \
    '{"matchers":[],"labelNames":["service_name","__profile_type__"]}'
  assert_absent "${stage}-metrics-isolation" \
    'http://127.0.0.1:19090/api/v1/query?query=alloy_build_info' alloy
  assert_absent "${stage}-logs-isolation" \
    "http://127.0.0.1:13101/loki/api/v1/query_range?query=%7Bjob%3D%22${marker}%22%7D" "${marker}"
  assert_absent "${stage}-traces-isolation" \
    "http://127.0.0.1:13201/api/search?q=%7Bresource.service.name%3D%22${marker}%22%7D&start=0&end=${now_s}" "${marker}"
  assert_absent "${stage}-profiles-isolation" \
    'http://127.0.0.1:14042/querier.v1.QuerierService/Series' alloy \
    '{"matchers":[],"labelNames":["service_name","__profile_type__"]}'
}

assert_no_restarts() {
  local stage=$1
  kubectl -n "${namespace}" get pods --field-selector=status.phase!=Succeeded \
    -o jsonpath='{range .items[*]}{.metadata.name}{" "}{range .status.containerStatuses[*]}{.restartCount}{" "}{end}{"\n"}{end}' \
    >"${evidence_dir}/${stage}-restarts.txt"
  awk '{ for (i = 2; i <= NF; i++) if ($i != 0) exit 1 }' \
    "${evidence_dir}/${stage}-restarts.txt"
}

send_corpus
query_corpus old
assert_no_restarts old

# These roles carry no singleton WAL partition or local durable ownership.
scalable=(metrics-distributor metrics-query-frontend logs-distributor \
  traces-distributor traces-querier traces-query-frontend profiles-distributor)
for workload in "${scalable[@]}"; do
  run kubectl -n "${namespace}" scale "deployment/${workload}" --replicas=2
  run kubectl -n "${namespace}" rollout status "deployment/${workload}" --timeout=10m
  query_corpus "scaled-${workload}"
done

# A singleton durability owner must block a normal node drain through its PDB.
protected_pod=$(kubectl -n "${namespace}" get pod -l app.kubernetes.io/name=metrics-block-builder -o jsonpath='{.items[0].metadata.name}')
protected_node=$(kubectl -n "${namespace}" get pod "${protected_pod}" -o jsonpath='{.spec.nodeName}')
if timeout 30s kubectl drain "${protected_node}" --ignore-daemonsets --delete-emptydir-data \
  >"${evidence_dir}/pdb.txt" 2>&1; then
  echo "protected node drain unexpectedly passed" >&2
  exit 1
fi
run kubectl uncordon "${protected_node}"
wait_deployments

# Direct pod deletion is not an eviction: recovery must replay from durable
# WAL/object storage and restore the same public answers.
run kubectl -n "${namespace}" delete pod "${protected_pod}" --wait=false
run kubectl -n "${namespace}" rollout status deployment/metrics-block-builder --timeout=10m
query_corpus pod-delete

# Exercise the node-loss recovery path after proving the normal eviction path
# is protected. Forced deletion is explicit here; the cold data remains on the
# node's PVCs and WAL/object-store owners are restarted before queries resume.
run kubectl drain "${protected_node}" --ignore-daemonsets --delete-emptydir-data --disable-eviction
run kubectl uncordon "${protected_node}"
run kubectl -n "${namespace}" rollout status statefulset/broker --timeout=10m
run kubectl -n "${namespace}" rollout status statefulset/minio --timeout=10m
wait_deployments
query_corpus node-drain

# Rolling N-1 -> N and back, one role at a time, while the public corpus stays
# queryable. The immutable images are both loaded into the kind node above.
mapfile -t deployments < <(kubectl -n "${namespace}" get deployments -l app.kubernetes.io/part-of=krabka-o11y -o name | sort)
for deployment in "${deployments[@]}"; do
  marker="${base_marker}-upgrade-${deployment##*/}"
  send_corpus
  run kubectl -n "${namespace}" set image "${deployment}" "role=${new_image}"
  run kubectl -n "${namespace}" rollout status "${deployment}" --timeout=10m
  query_corpus "upgrade-${deployment##*/}"
done
assert_no_restarts upgraded
for deployment in "${deployments[@]}"; do
  marker="${base_marker}-rollback-${deployment##*/}"
  send_corpus
  run kubectl -n "${namespace}" set image "${deployment}" "role=${old_image}"
  run kubectl -n "${namespace}" rollout status "${deployment}" --timeout=10m
  query_corpus "rollback-${deployment##*/}"
done
assert_no_restarts rolled-back

# Cold restart every Krabka role while the broker and object store retain their
# PVCs, then prove the acknowledged corpus is still queryable.
for deployment in "${deployments[@]}"; do
  run kubectl -n "${namespace}" scale "${deployment}" --replicas=0
done
for deployment in "${deployments[@]}"; do
  run kubectl -n "${namespace}" scale "${deployment}" --replicas=1
done
wait_deployments
query_corpus cold-restart
assert_no_restarts cold-restart

kubectl -n "${namespace}" get all -o wide >"${evidence_dir}/objects.txt"
docker image inspect "${new_image}" >"${evidence_dir}/new-image.json"
docker image inspect "${old_image}" >"${evidence_dir}/old-image.json"
printf 'result=passed\ncluster=%s\nnew_image=%s\nold_image=%s\nmanifest_sha256=%s\n' \
  "${cluster}" "${new_image}" "${old_image}" \
  "$(sha256sum "${evidence_dir}/manifests.current.yaml" | cut -d ' ' -f 1)" \
  >"${evidence_dir}/report.txt"
find "${evidence_dir}" -maxdepth 1 -type f ! -name SHA256SUMS -print0 |
  sort -z | xargs -0 sha256sum >"${evidence_dir}/SHA256SUMS"
