# krabka-o11y

The [krabka](https://github.com/krabka-io) observability stack: metrics, traces,
profiles and logs, each with the query language its ecosystem already speaks.

It layers on three sibling repositories —
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) for ids and
units, [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) for
the Kafka clients each signal is shipped over, and
[`krabka-broker`](https://github.com/krabka-io/krabka-broker) for object
storage, telemetry and rate limiting. Nothing here compiles against the broker
itself; its integration suites boot one.

## Crates

One storage layer, one query language per signal, and one ingest-and-serve path
per signal above them.

| Crate | What it is |
| --- | --- |
| `krabka-blockstore` | The columnar block store every signal is written through, over object storage |
| `krabka-logql` | LogQL: Grafana Loki's log query language |
| `krabka-promql` | PromQL: Prometheus' metric query language, with a conformance corpus |
| `krabka-traceql` | TraceQL: Grafana Tempo's trace query language |
| `krabka-pprof` | The pprof profile format, read and written |
| `krabka-metrics` | Prometheus remote-write ingest, and the block builder behind it |
| `krabka-metrics-service` | The PromQL query API, answering what Grafana and Prometheus ask |
| `krabka-traces` | OTLP trace ingest and TraceQL serving |
| `krabka-profiles` | Continuous-profiling ingest and pprof serving |
| `krabka-observability` | The log path, and the surface that ties the four together |

## Build

```bash
cargo test --workspace
```

```bash
bazel test //...
```

Both are gated in CI, and they are not the same build. Bazel supplies its own
`protoc`, pins the container images the differential suites run against, and
runs the mutation sweep. The cargo job covers what only cargo reaches: the
`protoc-bin-vendored` fallback, `.cargo/config.toml`, `--locked` against
`Cargo.lock`, and the `heap-profiling` feature. CI also runs `cargo deny check`
over the policy in [`deny.toml`](deny.toml).

## Run

The repository publishes one `linux/amd64` image that holds the five service
binaries and `krabka-o11y-bootstrap`. It sets no entrypoint, so a deployment
names the binary it runs. Use an immutable digest:

```bash
docker run --rm ghcr.io/krabka-io/krabka-o11y@sha256:<digest> \
  krabka-metrics --target=distributor --bootstrap=broker:9092
```

Bazel builds the image from the same targets `bazel test //...` tests:

```bash
bazel run //bazel/images/krabka:load     # loads krabka-o11y:dev into Docker
```

Every signal names its stages with one vocabulary -- `distributor`,
`block-builder`, `querier`, `query-frontend`, `compactor`, and the ones only
one signal has -- taken from Loki, Mimir, Tempo and Pyroscope. `--target all`
runs every role of a signal in one process on one port, which is the shape to
evaluate the stack in:

```bash
docker run --rm ghcr.io/krabka-io/krabka-o11y@sha256:<digest> \
  krabka-observability --target=all --wal-bootstrap-server=broker:9092
```

[`deploy/`](deploy) holds a Docker Compose stack and a kustomize base that run
one role of each signal against a broker and an object store. Read
[`deploy/README.md`](deploy/README.md) first. It records which lifecycle
property of the binaries each probe, grace period, and volume is wired to, and
which three of them do not hold.

```bash
docker compose -f deploy/compose/docker-compose.yaml up -d
kubectl apply -k deploy
```

[`krabka-o11y-demo`](https://github.com/krabka-io/krabka-o11y-demo/tree/main/demo/observability)
is a larger stack around the same image. It adds Grafana, Alloy and an
instrumented workload, so it shows the signals rather than only serving them.

Provision the topic contract before you start a service:

```bash
docker run --rm ghcr.io/krabka-io/krabka-o11y@sha256:<digest> \
  krabka-o11y-bootstrap --bootstrap broker:9092
```

The command creates missing topics and rejects an incompatible existing topic.
Set `--partitions`, `--replicas`, and `--retention-ms` for the deployment.
Do not change the partition count after data is written. The WAL partition is
part of the per-series ordering contract. No service binary makes this check
for itself, so run the command before every role, as the manifests in
`deploy/` do.

| Topic | Policy | Purpose |
| --- | --- | --- |
| `__krabka_metrics_wal` | delete | Metrics WAL |
| `__krabka_traces_wal` | delete | Traces WAL |
| `__krabka_profiles_wal` | delete | Profiles WAL |
| `__krabka_observability_logs_wal` | delete | Logs WAL |
| `__krabka_metrics_ha` | compact | HA tracker state |
| `__krabka_metrics_ruler_state` | compact | Alert state |

## Differential suites

Nine suites boot a real Grafana-stack component and compare against it, rather
than against a fixture of what it was once believed to do:

| Suite | Compares against |
| --- | --- |
| `metrics-service/diff_prometheus` | Prometheus |
| `metrics-service/diff_mimir` | Grafana Mimir |
| `metrics-service/grafana_integration` | Grafana |
| `traces/tempo_differential` | Grafana Tempo |
| `traces/grafana_e2e` | Grafana, Prometheus |
| `observability/loki_differential` | Grafana Loki |
| `observability/grafana_integration` | Grafana |
| `observability/grafana_e2e` | Grafana, Grafana Loki |
| `profiles/pyroscope_differential` | Grafana Pyroscope |

They need a Docker daemon and are tagged `docker`, which keeps them out of a
plain `bazel test //...`:

```bash
bazel test --config=docker //crates/metrics-service:diff_prometheus_docker_test
```

The images are pinned by digest in [`MODULE.bazel`](MODULE.bazel) and loaded
from a tarball before the suite runs, so nothing is pulled mid-test. Several of
these suites previously defaulted to `:latest`, which is what digest pinning
exists to remove: a differential test that disagrees with a moving target
reports a difference nobody made.

## protoc

`krabka-metrics` and `krabka-profiles` generate prost types from vendored
protos. Their `build.rs` uses `$PROTOC` when the build system supplies one and
falls back to the `protoc-bin-vendored` crate otherwise, which is what a plain
`cargo build` does.

Bazel supplies its own and turns that fallback off. The vendored crates locate
their binary through `env!("CARGO_MANIFEST_DIR")`, which bakes an absolute
build path into the artifact — the same sources would produce different bytes
on different machines, and a sandboxed build refuses it.

## Mutation testing

```bash
bazel test //crates/promql:promql_mutants
```

Sharded, and bounded per shard. A shard that overruns its bound reports
*nothing* rather than reporting a failure, so a survivor count is only worth
quoting once the totals line adds up — `caught + missed + unviable == total`.

## Publishing

These crates are not published from here. `robot-head/crabka` still owns every
`krabka-*` name on crates.io; this repository is where the observability stack
is developed.
