# Krabka — project-specific guidance

`krabka-o11y` is an observability stack: metrics, traces, profiles and logs, each served through the query language its ecosystem already speaks (PromQL, TraceQL, pprof, LogQL). Ten crates under `crates/`, one storage layer (`krabka-blockstore`) beneath them. It does not compile against the Krabka broker; its integration suites boot one.

## Commands

CI gates on both Bazel and Cargo, and they are not the same build. Bazel is the primary lane: it supplies its own `protoc`, pins the differential suites' container images, and runs the mutation sweep. The `cargo` job covers what only Cargo reaches -- the `protoc-bin-vendored` fallback, `.cargo/config.toml`, `--locked` against `Cargo.lock`, and the `heap-profiling` feature that no Bazel target turns on.

```bash
bazel run //tools/format     # rewrite formatting (replaces `cargo +nightly fmt --all`)
bazel test //...             # build + unit/integration tests + clippy
```

Those two, in that order, are the pre-commit check. There is no `Makefile`, `justfile`, `xtask`, or git hook, and no `CONTRIBUTING.md`.

`bazel test //...` excludes the six container suites (`.bazelrc` sets `--test_tag_filters=-docker`). Run one explicitly when you touch its code:

```bash
bazel test --config=docker //crates/metrics-service:diff_prometheus_docker_test
```

Scoped equivalents for a fast inner loop on one crate:

```bash
cargo test -p krabka-promql
cargo clippy -p krabka-promql --all-targets -- -D warnings
```

**Do not run these casually.** They are scheduled or nightly jobs:

| Command | Cost |
| --- | --- |
| `bazel build //...` cold | 90-min CI budget; fetches LLVM, JDK 21, protoc, and a ~340 MB DataFusion clone |
| `bazel test //crates/<crate>:<crate>_mutants` | Rebuilds the crate once per mutant; 180-min budget for promql/observability/traces |
| `tools/mutants-sweep.sh` | 10-hour timeout; has OOM-killed a 31 GB machine twice |
| `bazel coverage //crates/...` | Separate 90-min job; evicts the normal build cache |
| `tools/bench.sh` | Criterion over //benches, minutes per target; builds the DataFusion pin at opt-level 3 into a second target dir. `tools/bench.sh --quick` is the local mode |
| `bazel test --config=scale //crates/...` | The `scale` suites: 10k series, 1M spans, a 24-block compaction, and a bounded ingest-and-query soak, all against a real MinIO. Its own nightly job |

## Compatibility

**Krabka is greenfield and undeployed.** There are no production users, no persisted state to migrate, and no clients pinned to a specific build. Do not write backwards-compatibility shims:

- No `#[serde(default)]` on record fields "to keep old WAL records readable"
- No `V2` enum variants that stay alongside `V1` to support replay
- No feature flags that gate new behavior behind a default-off switch
- No migration code or one-shot upgraders for on-disk format changes
- No deprecated-but-kept API surfaces

When a schema, enum, wire format, or interface changes, change it. Delete local WAL topics, blocks, and data directories during development if necessary.

**Upstream compatibility is the constraint that matters.** Each signal has one upstream implementation that Krabka must match:

| Signal | Upstream | Surface to match |
| --- | --- | --- |
| Metrics | Prometheus, Grafana Mimir | PromQL semantics, the `/api/v1/*` query API, remote-write ingest |
| Logs | Grafana Loki | LogQL semantics, the `/loki/api/v1/*` query API |
| Traces | Grafana Tempo | TraceQL semantics, the `/api/search` and `/api/traces` API, OTLP ingest |
| Profiles | Grafana Pyroscope | The pprof profile format, and the ingest and query API |

Always keep the query-language semantics, the HTTP response shapes and status codes, and the wire formats these define. Grafana must be able to point a datasource at Krabka and see what it would see from the upstream component.

**The oracle is the upstream implementation running in a container, not its documentation.** Two kinds of executable artifact hold that oracle, and both are in this repository:

- **Nine differential suites.** They boot the real component and compare against it: `diff_prometheus`, `diff_mimir` and `grafana_integration` in `crates/metrics-service`, `tempo_differential` and `grafana_e2e` in `crates/traces`, `loki_differential`, `grafana_integration` and `grafana_e2e` in `crates/observability`, and `pyroscope_differential` in `crates/profiles`. `MODULE.bazel` pins each image by digest and Bazel loads it from a tarball, so a suite never compares against a moving target.
- **Vendored conformance corpora.** `crates/promql/tests/testdata/` holds a curated subset of the Prometheus `promql` test corpus, and its `ATTRIBUTION.md` records the upstream tag and commit each file came from. `crates/traceql/tests/testdata/traceql/` holds the TraceQL golden corpus.

When in doubt, match the upstream. If the upstream behavior is undocumented or version-dependent, run the pinned image and observe it. Do not rely on a blog post or a wiki. When you deliberately diverge, say so where the divergence lives, and say why, as `ATTRIBUTION.md` does for the corpus cases that Krabka does not implement.

**Kafka is transport here, not a contract.** Each signal's write-ahead log is a Kafka topic that the `krabka-client-*` crates write and read. `krabka-metrics` and `krabka-traces` take `krabka-broker` as a dev-dependency and start one in process, so their `ingest_roundtrip` suites can exercise that path. No library or binary target depends on the broker. Krabka does not implement the Kafka protocol, and nothing here is checked against a Kafka oracle.

## Code & Documentation Style

Style guides live in [`docs/style_guides/`](docs/style_guides/README.md). Read by need, not all at once:

| Read | Guide | When |
| --- | --- | --- |
| Always | [prose](docs/style_guides/prose_style_guide.md) (6 KB) | Any prose you write, including commit messages and PR bodies |
| Writing Rust | [code](docs/style_guides/code_style_guide.md) (31 KB) | Toolchain, lints, naming, imports, errors, tests |
| Public items | [rustdoc](docs/style_guides/rustdoc_style_guide.md) | `///` and `//!` conventions |
| Authoring that doc type only | [README](docs/style_guides/readme_style_guide.md), [design docs](docs/style_guides/design_doc_style_guide.md), [coverage reports](docs/style_guides/coverage_report_style_guide.md) | Templates, not day-to-day rules |

Do not make style-only sweeps across untouched files. Bring a file into line with the guides only when you already edit it. Keep the tidy-up proportionate to the change.

### Clippy

Never add `#[allow(clippy::...)]` or any equivalent Clippy suppression. Fix every Clippy warning in the code, regardless of the effort required.

## Testing

Never use Rust's plain `assert!`, `assert_eq!`, or `assert_ne!` macros. Use the `assert2` crate's `assert!` macro instead. Use it also for equality and inequality comparisons.

Tests must exercise behavior, not source text. Do not read source files in tests and assert against their contents. `include_str!` and `fs::read_to_string` are examples of such reads. If a behavior is hard to test, add a narrow helper or seam. Then test that behavior directly.

When you check generated protocol records or other structured values in tests, compare the whole expected struct. This is better than long chains of field-by-field assertions. Use table-driven or parameterized tests for repeated scenarios that differ only by inputs, protocol version, or expected request shape.

## Execution

When you execute an implementation plan, always use **subagent-driven development in parallel batches** where the per-task file sets do not overlap. The plan groups tasks into batches. Dispatch all tasks in a batch concurrently, in one message with multiple Agent calls. Then wait for the batch to complete, review it, and move to the next batch.

Sequential dispatch of one task at a time wastes wall-clock time. Use sequential dispatch only when later tasks depend on earlier ones in the same batch.

A "conflict" between parallel implementers occurs only when both edit the same file. Tasks such as "add a PromQL function" in `crates/promql/src/functions/` and "add a TraceQL operator" in `crates/traceql/src/` do not conflict, and you should run them together. When in doubt, list the file set that each task touches before you decide.

**Never discard working-tree state while parallel implementers run.** `git checkout -- <path>`, `git restore`, `git stash`, and `git clean` all destroy *every* uncommitted change in the files they touch, not only yours. In a shared worktree, those files usually hold the unfinished work of another agent. To undo your own edit, reverse it directly. Re-edit the region, or apply a reverse patch of your own diff. This has already destroyed the uncommitted work of one agent.

## Release Process

Write conventional commits. They are the repo's convention and are applied consistently:

- `feat:` — a feature, at the minor level
- `fix:` — a fix, at the patch level
- `feat!:` — a breaking change, at the major level

No crate in this repo is published — all ten set `publish = false`, because they depend on a git pin of DataFusion that crates.io rejects. There is no release automation configured here.
