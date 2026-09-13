# Getting Started

This guide starts Krabka locally and queries metrics, logs, traces, and profiles.

## Requirements

Install Docker with the Compose plugin, Bazelisk, and `curl`.

## Start the stack

Build the same image that CI tests, then start the checked-in Compose deployment:

```bash
bazel run //bazel/images/krabka:load
KRABKA_O11Y_IMAGE=krabka-o11y:dev \
  docker compose -f deploy/compose/docker-compose.yaml up -d
```

The bootstrap containers create and validate the six broker topics before the services start.

Wait until the containers are healthy:

```bash
docker compose -f deploy/compose/docker-compose.yaml ps
```

Use one tenant for this guide:

```bash
export KRABKA_TENANT=quickstart
```

Every data request must carry `X-Scope-OrgID`.

## Metrics

Point Prometheus agent mode or Grafana Alloy remote write at `http://localhost:4041/api/v1/push` and add this header:

```yaml
headers:
  X-Scope-OrgID: quickstart
```

Krabka does not scrape targets and does not ship a collection agent.

Query the Prometheus API after the agent sends a sample:

```bash
curl -fsS -G http://localhost:9090/api/v1/query \
  -H "X-Scope-OrgID: ${KRABKA_TENANT}" \
  --data-urlencode 'query=up'
```

## Logs

Send one Loki push request:

```bash
curl -fsS http://localhost:3100/loki/api/v1/push \
  -H "X-Scope-OrgID: ${KRABKA_TENANT}" \
  -H 'Content-Type: application/json' \
  --data "{\"streams\":[{\"stream\":{\"job\":\"quickstart\"},\"values\":[[\"$(date +%s%N)\",\"hello from Krabka\"]]}]}"
```

Query the Loki API:

```bash
curl -fsS -G http://localhost:3101/loki/api/v1/query_range \
  -H "X-Scope-OrgID: ${KRABKA_TENANT}" \
  --data-urlencode 'query={job="quickstart"}'
```

## Traces

Point an OTLP exporter at `http://localhost:4318` for HTTP or `localhost:4317` for gRPC and send `X-Scope-OrgID=quickstart` as OTLP metadata.

Perform a TraceQL search through the Tempo API:

```bash
curl -fsS -G http://localhost:3201/api/search \
  -H "X-Scope-OrgID: ${KRABKA_TENANT}" \
  --data-urlencode 'q={}' \
  --data 'start=0' \
  --data "end=$(date +%s)"
```

Jaeger, Zipkin, and OTLP are ingest protocols.

Use the Tempo API or Grafana Tempo datasource for trace reads because Krabka does not serve the Jaeger query API.

## Profiles

Point Grafana Alloy `pyroscope.write` at `http://localhost:4040` and add `X-Scope-OrgID=quickstart` as a request header.

Query a CPU flame graph through the Pyroscope render API:

```bash
curl -fsS -G http://localhost:4042/pyroscope/render \
  -H "X-Scope-OrgID: ${KRABKA_TENANT}" \
  --data-urlencode 'query=cpu:cpu:nanoseconds:cpu:nanoseconds{}' \
  --data 'from=now-1h' \
  --data 'until=now'
```

Grafana uses the Connect query API for labels, series, and profile merges.

Krabka deliberately does not serve the legacy `/pyroscope/labels` or `/pyroscope/label-values` routes.

## Add Grafana

Follow [Grafana datasource setup](grafana.md) for all four datasource definitions.

Read [Operations](operations.md) before a durable or multi-node deployment.
