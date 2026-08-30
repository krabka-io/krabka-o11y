# Krabka — project-specific guidance

`krabka-o11y` is an observability stack: metrics, traces, profiles and logs, each served through the query language its ecosystem already speaks (PromQL, TraceQL, pprof, LogQL). Ten crates under `crates/`, one storage layer (`krabka-blockstore`) beneath them. It does not compile against the Krabka broker; its integration suites boot one.

## Commands

CI gates on Bazel and runs no `cargo`. Cargo works and is supported, but it is not what the repo is checked against.

```bash
bazel run //tools/format     # rewrite formatting (replaces `cargo +nightly fmt --all`)
bazel test //...             # build + unit/integration tests + clippy
```

Those two, in that order, are the pre-commit check. There is no `Makefile`, `justfile`, `xtask`, or git hook, and the `CONTRIBUTING.md` that the style guides link to does not exist.

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

## Compatibility

**Krabka is greenfield and undeployed.** There are no production users, no persisted state to migrate, and no clients pinned to a specific build. Do not write backwards-compatibility shims:

- No `#[serde(default)]` on metadata fields "to keep old raft logs readable"
- No `V2` enum variants that stay alongside `V1` to support replay
- No feature flags that gate new behavior behind a default-off switch
- No migration code or one-shot upgraders for on-disk format changes
- No deprecated-but-kept API surfaces

When a schema, enum, wire format, or interface changes, change it. Delete local raft logs and data directories during development if necessary.

**Kafka compatibility is the constraint that matters.** Always keep:

- Apache Kafka wire-protocol byte exactness for request and response shapes, field order, error codes, and version negotiation
- KIP semantics for the feature that you implement
- Behavior that the JVM admin tools rely on, such as `kafka-topics`, `kafka-acls`, `kafka-leader-election`, and `kafka-reassign-partitions`

When in doubt, match Kafka. If Kafka's behavior is undocumented or version-dependent, check the behavior of the latest released cp-kafka image. Do not rely on the wiki.

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

A "conflict" between parallel implementers occurs only when both edit the same file. Tasks such as "add wire codes" in codes.rs and "add metadata fields" in records.rs do not conflict, and you should run them together. When in doubt, list the file set that each task touches before you decide.

**Never discard working-tree state while parallel implementers run.** `git checkout -- <path>`, `git restore`, `git stash`, and `git clean` all destroy *every* uncommitted change in the files they touch, not only yours. In a shared worktree, those files usually hold the unfinished work of another agent. To undo your own edit, reverse it directly. Re-edit the region, or apply a reverse patch of your own diff. This has already destroyed the uncommitted work of one agent.

## Release Process

Write conventional commits. They are the repo's convention and are applied consistently:

- `feat:` — a feature, at the minor level
- `fix:` — a fix, at the patch level
- `feat!:` — a breaking change, at the major level

No crate in this repo is published — all ten set `publish = false`, because they depend on a git pin of DataFusion that crates.io rejects. There is no release automation configured here.
