#!/usr/bin/env bash
set -euo pipefail

render_dir=$(mktemp -d)
trap 'rm -r "${render_dir}"' EXIT
kubectl kustomize deploy >"${render_dir}/first.yaml"
kubectl kustomize deploy >"${render_dir}/second.yaml"
cmp "${render_dir}/first.yaml" "${render_dir}/second.yaml"

ruby -ryaml -e '
docs = YAML.load_stream(File.read(ARGV.fetch(0))).compact
workloads = docs.select { |doc| %w[Deployment StatefulSet Job].include?(doc["kind"]) }
raise "expected 25 workloads, found #{workloads.size}" unless workloads.size == 25
workloads.each do |workload|
  name = workload.dig("metadata", "name")
  spec = workload.dig("spec", "template", "spec")
  raise "#{name}: service account token is mounted" unless spec["automountServiceAccountToken"] == false
  spec.fetch("containers").each do |container|
    container_name = container.fetch("name")
    raise "#{name}/#{container_name}: missing resources" unless container.dig("resources", "requests") && container.dig("resources", "limits")
    raise "#{name}/#{container_name}: missing security context" unless container["securityContext"]
    next if workload["kind"] == "Job"
    raise "#{name}/#{container_name}: missing readiness probe" unless container["readinessProbe"]
    raise "#{name}/#{container_name}: missing liveness probe" unless container["livenessProbe"]
  end
  spec.fetch("initContainers", []).each do |container|
    container_name = container.fetch("name")
    raise "#{name}/#{container_name}: missing resources" unless container.dig("resources", "requests") && container.dig("resources", "limits")
    raise "#{name}/#{container_name}: missing security context" unless container["securityContext"]
  end
end
pdbs = docs.count { |doc| doc["kind"] == "PodDisruptionBudget" }
raise "expected 24 disruption budgets, found #{pdbs}" unless pdbs == 24
traces = docs.find { |doc| doc["kind"] == "Service" && doc.dig("metadata", "name") == "traces-distributor" }
trace_ports = traces.fetch("spec").fetch("ports").map { |port| [port.fetch("name"), port.fetch("protocol", "TCP")] }
expected_trace_ports = [["http", "TCP"], ["otlp-grpc", "TCP"], ["otlp-http", "TCP"], ["jaeger-grpc", "TCP"], ["jaeger-compact", "UDP"], ["jaeger-http", "TCP"], ["zipkin", "TCP"]]
raise "traces-distributor: ingest ports are incomplete" unless trace_ports.sort == expected_trace_ports.sort
wal_handoff = %w[metrics-block-builder metrics-querier profiles-block-builder profiles-querier profiles-query-frontend traces-block-builder traces-live-store traces-metrics-generator]
wal_handoff.each do |name|
  deployment = docs.find { |doc| doc["kind"] == "Deployment" && doc.dig("metadata", "name") == name }
  rolling = deployment.dig("spec", "strategy", "rollingUpdate")
  raise "#{name}: WAL handoff can surge" unless rolling == {"maxSurge" => 0, "maxUnavailable" => 1}
end
' "${render_dir}/first.yaml"
