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
| `krabka-query-frontend` | Shared query planning, fan-out, retry, cache and merge orchestration |
| `krabka-integration` | Cross-signal integration tests |

## Documentation

- [Website and complete documentation](https://krabka.io/krabka-o11y/)
- [WASI observability lab](https://krabka.io/krabka-o11y/lab/)
- [Getting started](docs/getting_started.md)
- [Observing Krabka clusters](docs/observing_krabka_clusters.md)
- [Operations](docs/operations.md)
- [Measured operating envelope](docs/operating_envelope.md)
- [Architecture](docs/architecture_design.md)
- [Grafana datasource setup](docs/grafana.md)
- [Test coverage](docs/test_coverage_report.md)
- [Rust API reference](https://krabka.io/krabka-o11y/api/)
- [Build the website and browser lab](website/README.md)

## Feature compatibility

The [API compatibility matrix](docs/api_compatibility.md) maps every supported surface to a differential suite.

[Query-language qualification](docs/query_language_conformance_proposal.md) records pinned corpora, upstream runners, storage-transition checks and remaining semantic gaps.

The generated [route inventory](docs/api/routes.json) lists every served HTTP method and path.

## Build

```bash
cargo test --workspace
```

```bash
bazel test //...
```

Both are gated in CI, and they are not the same build. Bazel supplies its own
`protoc`, pins the container images the differential suites run against, and
defines the mutation targets. The cargo job covers what only cargo reaches: the
`protoc-bin-vendored` fallback, `.cargo/config.toml`, `--locked` against
`Cargo.lock`, and the `heap-profiling` feature. CI also runs `cargo deny check`
over the policy in [`deny.toml`](deny.toml).

## Run

The repository publishes a `linux/amd64` and `linux/arm64` image index for each
release and for `latest`. The image holds the five service binaries and
`krabka-o11y-bootstrap`. It sets no entrypoint, so a deployment names the
binary it runs. Use an immutable digest:

```bash
docker run --rm ghcr.io/krabka-io/krabka-o11y@sha256:<digest> \
  krabka-metrics --target=distributor --bootstrap=broker:9092
```

Bazel builds the image from the same targets `bazel test //...` tests:

```bash
bazel run //bazel/images/krabka:load     # loads krabka-o11y:dev into Docker
```

On an ARM64 host, including an Apple Silicon Mac running Docker Desktop, build
and load a native image locally by selecting the Linux ARM64 platform
explicitly:

```
bazel run --platforms=//:linux_arm64 //bazel/images/krabka:load
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

[`deploy/`](deploy) holds a Docker Compose stack and a production-role
kustomize base against a broker and an object store. Read
[`deploy/README.md`](deploy/README.md) first. It records which lifecycle
property of the binaries each probe, grace period, and volume is wired to.

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
quoting once `tools/mutants-ratchet.py` validates every shard and the totals
line adds up: `caught + missed + unviable == total`.

Mutation sweeps do not run in CI, not even on a schedule. A sweep takes hours
and holds a machine for the whole run. Run one by hand on a dedicated host and
apply the ratchet to its output:

```bash
tools/mutants-sweep.sh promql
tools/mutants-ratchet.py promql
```

The ratchet's exit code names its verdict. 0 is a pass. 1 is a regression: a
complete sweep has more survivors than its baseline allows. 3 is an incomplete
sweep: a shard was killed, was silent, refused, or its numbers disagree, so the
survivor count is unknown. Rerun the missing shards, and do not change the
baseline. 2 is a wrong command line. `--json <path>` writes the same verdict,
with each crate's counts and reasons.

`tools/mutants-ratchet.py --prove-gate <crate>` shows that the checked-in
baseline rejects one new survivor. It writes synthetic shard logs in the shape
of the crate's recorded sweep and runs no sweep. The `deliberate_survivor`
block in
[`qualification/milestone-19-mutation-baselines.json`](qualification/milestone-19-mutation-baselines.json)
is its output.

CI runs only checks that read files: the ratchet's `--self-test`, which checks
the verdict logic against synthetic shard logs in about a second, and
`tools/mutants-record.py --check`, which makes sure that each number in
`tools/mutants-baseline.txt` matches a recorded run with its commit, toolchain,
host shape, command, duration, and checksums.

### Seeding a crate by hand

Eight crates have a baseline. observability, profiles, promql, and traces are
still `unseeded`. Their sweeps are the largest, and a partial run gives no
number. Seed each one on a dedicated host:

1. Get a host that does no other work for the full run. A sweep of one of these
   crates can take up to the 10-hour shard timeout, and
   `tools/mutants-sweep.sh` has twice stopped a 31 GB machine because it ran
   out of memory. Each concurrent shard links with `ld.lld`, which holds 1.5 to
   2 GB. Use at least 64 GB of memory, or set `KRABKA_MUTANTS_CONCURRENT_SHARDS`
   lower (the default is the CPU count, to a maximum of 8). Keep about 200 GB
   of disk free for the Bazel output base.
2. Check out the commit to record, with a clean working tree. Do not change
   `mutants_shards` in the crate's BUILD file. The ratchet rejects logs from a
   different shard count.
3. Run the sweep and the ratchet:

   ```bash
   tools/mutants-sweep.sh promql
   tools/mutants-ratchet.py promql --json promql-verdict.json
   ```

   The sweep writes `promql.log`, `promql.metadata.txt` and
   `promql.SHA256SUMS` to `~/krabka-work/sweep-results`, not to `/tmp`. Exit 3
   means the sweep is incomplete. Rerun it, and record nothing.
4. Archive the shard logs and store the archive where the team can get it:

   ```bash
   tar -C bazel-testlogs/crates/promql -caf ~/krabka-work/sweep-results/promql.tar.zst promql_mutants
   gcloud storage cp ~/krabka-work/sweep-results/promql.tar.zst gs://<bucket>/qualification/milestone-19/mutants/<commit>/
   ```

5. Record the run:

   ```bash
   tools/mutants-record.py --capture promql \
     --artifact ~/krabka-work/sweep-results/promql.tar.zst \
     --artifact-url gs://<bucket>/qualification/milestone-19/mutants/<commit>/promql.tar.zst \
     --runner-label <host> --write
   ```

   It applies the ratchet's structural gate again and reads the metadata and
   checksum files. It takes the SHA-256 of the archive and of
   `promql.SHA256SUMS`, and writes the result into the JSON record. It removes
   the crate from `unseeded` and writes the survivor count into
   `tools/mutants-baseline.txt`. Then it runs `--check`.
6. Update the crate's `test_coverage_report.md` and
   [`docs/test_coverage_report.md`](docs/test_coverage_report.md), and commit
   the record, the baseline, and the reports together.

## Publishing

These crates are not published to crates.io from here. `robot-head/crabka`
still owns every `krabka-*` name there.

CI publishes the rendered workspace rustdoc to
[`krabka-io.github.io/krabka-o11y`](https://krabka-io.github.io/krabka-o11y/).
