# Observing Krabka Clusters

This guide connects a Krabka broker cluster to the krabka-o11y stack. It moved
here from the [krabka.io](https://krabka.io) website, which now covers the
broker and the Kafka ecosystem only. krabka-o11y is part of the broader Krabka
ecosystem and has its own documentation in this repository.

## What the broker exports

The broker exports its own telemetry through the `krabka-telemetry` crate in
[krabka-broker](https://github.com/krabka-io/krabka-broker). No sidecar or JMX
exporter is needed.

| Signal | Broker surface | Notes |
| --- | --- | --- |
| Metrics | Prometheus text format at `/metrics` on the metrics listener | `--metrics-listen-addr` or `KRABKA_METRICS_LISTEN_ADDR` sets the address. The default is `0.0.0.0:9404`. |
| Traces | OTLP export | The `KRABKA_OTLP_*` environment variables turn tracing on. |
| Profiles | pprof routes on the admin listener | `GET /debug/pprof/profile` gives a CPU profile. `GET /debug/pprof/heap` gives a heap profile when the broker is built with the `heap-profiling` feature. `KRABKA_ADMIN_LISTEN_ADDR` sets the address. |
| Logs | Structured JSON or logfmt | The `krabka-logfmt` crate encodes each line as key-value pairs. |
| Audit | Hash-chained OCSF records on the `__krabka_audit` topic | The `krabka-audit` crate writes them. Its verify command checks the chain. krabka-o11y does not ingest this topic. |

Trace context crosses the wire in record headers with the W3C `traceparent`
format from the `krabka-trace-context` crate.

## Where each signal lands

krabka-o11y stores all four signals through one columnar block store over
object storage. Each signal keeps the query language its ecosystem already
speaks. Audit records stay on their Kafka topic and are not a krabka-o11y
signal.

| Signal | Ingest | Query interface | Crates |
| --- | --- | --- | --- |
| Metrics | Prometheus remote write | PromQL | `krabka-promql`, `krabka-metrics`, `krabka-metrics-service` |
| Traces | OTLP | TraceQL through the Tempo API | `krabka-traceql`, `krabka-traces` |
| Logs | Loki push | LogQL | `krabka-logql`, `krabka-observability` |
| Profiles | Pyroscope push | pprof | `krabka-pprof`, `krabka-profiles` |

[Getting started](getting_started.md) starts the stack and sends one request
per signal. [Grafana datasources](grafana.md) provisions Grafana against it.

## Broker metrics in PromQL

These expressions read the broker's `/metrics` output after a Prometheus agent
or Grafana Alloy forwards it to krabka-o11y.

```promql
# Message ingress rate for the whole cluster
sum(rate(crabka_broker_messages_in_total[5m]))

# Byte ingress rate per topic
sum by (topic) (rate(crabka_broker_topic_bytes_in_total[5m]))

# KRaft controller leader changes
sum(crabka_broker_controller_leader_changes_total)

# In-sync replica shrink rate
sum(rate(crabka_broker_isr_shrinks_total[5m]))
```

## Demo stack

The [krabka-o11y-demo](https://github.com/krabka-io/krabka-o11y-demo)
repository runs a broker, the krabka-o11y services, Grafana, and an
instrumented orders pipeline in one Compose file:

```bash
git clone https://github.com/krabka-io/krabka-o11y-demo
cd krabka-o11y-demo/demo/observability
docker compose up -d
```

Grafana serves at `http://localhost:3000` with anonymous admin access. The
provisioned dashboards read the PromQL expressions above.
