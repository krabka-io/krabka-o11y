# Operations

This guide covers the operating contracts that are easy to miss in a working deployment.

## Configuration

Each binary accepts command-line flags, environment variables, and a YAML file.

A flag wins over its environment variable, and an environment variable wins over the YAML value.

The checked-in files under [`deploy/roles/`](../deploy/roles) are the current role examples and are validated against each binary in CI.

Run a binary with `--help` for the complete flag and environment list.

## Tenancy and security

Send `X-Scope-OrgID` on every data request and configure the same tenant header in each Grafana datasource.

Treat telemetry bodies, labels, query strings, compressed payloads, and broker records as untrusted input.

Use TLS and the credentials-file options on exposed listeners.

Store object-store credentials in `AWS_*` environment variables rather than role YAML files.

## Topics

Run `krabka-o11y-bootstrap` before the roles start.

The command creates missing topics and rejects a topic whose partitions, replicas, retention, or cleanup policy do not meet the contract.

Do not change a WAL topic's partition count after data is written because partitioning preserves per-series order.

## Collection and sampling

Krabka has no first-party collection agent.

Use Prometheus agent mode or Grafana Alloy for metrics collection, and use an OpenTelemetry Collector or Grafana Alloy for logs, traces, and profiles.

The traces distributor stores every accepted span and does not perform head or tail sampling.

Apply head or tail sampling in the OpenTelemetry Collector before data reaches Krabka.

Ingest rate limits reject requests with HTTP 429 and do not sample them.

## Compatibility boundaries

Jaeger, Zipkin, and OTLP are trace ingest surfaces.

Read traces with the Tempo API, TraceQL, or the Grafana Tempo datasource.

The Pyroscope Connect API is the profile query surface used by Grafana.

The legacy profile render routes remain available, but the legacy label routes are out of scope.

See the [API compatibility matrix](api_compatibility.md) and the generated [route inventory](api/routes.json) for the exact surface.

## Readiness and shutdown

Use `/ready` for readiness and a TCP probe on the admin port for liveness.

Readiness names each unmet gate and returns 503 until the role can do its assigned work.

Give each role enough termination grace to drain its active batch.

For `--target all`, set `--all-drain-stage-timeout` below the orchestrator termination grace period.

The logs distributor uses `POST /ingester/prepare_shutdown` to stop new writes before exit.

## Storage and recovery

Persist broker data, logs manifests, and the logs delete-request store.

Other roles rebuild their state from the broker and object store.

Monitor consumer lag because readiness reports attachment, not catch-up progress.

Keep a metrics querier connected to the WAL with `--wal-bootstrap`; without it, recent samples remain invisible until block building completes.

## Verification

Run the base deployment checks with:

```bash
bazel test //deploy:all
```

Run all upstream differential suites with a Docker daemon:

```bash
bazel test --config=docker //crates/...
```

The detailed lifecycle limits of the Compose and Kubernetes manifests are in [`deploy/README.md`](../deploy/README.md).
