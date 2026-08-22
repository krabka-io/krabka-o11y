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
| `crabka-blockstore` | The columnar block store every signal is written through, over object storage |
| `crabka-logql` | LogQL: Grafana Loki's log query language |
| `crabka-promql` | PromQL: Prometheus' metric query language, with a conformance corpus |
| `crabka-traceql` | TraceQL: Grafana Tempo's trace query language |
| `crabka-pprof` | The pprof profile format, read and written |
| `crabka-metrics` | Prometheus remote-write ingest |
| `crabka-metrics-service` | The PromQL query API, answering what Grafana and Prometheus ask |
| `crabka-traces` | OTLP trace ingest and TraceQL serving |
| `crabka-profiles` | Continuous-profiling ingest and pprof serving |
| `crabka-observability` | The log path, and the surface that ties the four together |

## Build

```bash
cargo test --workspace
```

```bash
bazel test //...
```

Both are supported and both are gated in CI. Bazel additionally pins the
container images the differential suites run against, and runs the mutation
sweep.

## Differential suites

Six suites boot a real Grafana-stack component and compare against it, rather
than against a fixture of what it was once believed to do:

| Suite | Compares against |
| --- | --- |
| `metrics-service/diff_prometheus` | Prometheus |
| `metrics-service/diff_mimir` | Grafana Mimir |
| `metrics-service/grafana_integration` | Grafana |
| `traces/tempo_differential` | Grafana Tempo |
| `traces/grafana_e2e` | Grafana, Prometheus |
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

`crabka-metrics` and `crabka-profiles` generate prost types from vendored
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
`crabka-*` name on crates.io; this repository is where the observability stack
is developed.
