# Crate README Style Guide

This guide defines the style and content expectations for per-crate `README.md` files in Krabka. The [prose style guide](prose_style_guide.md) defines the wording rules that apply to everything you write here.

## Purpose

Each crate README is the **entry point for someone who sees the crate for the first time**. It answers: "what is this, why does it exist, and how do I use it?" No crate here is published, because every one of them depends on a git pin of DataFusion that crates.io rejects. So a README serves two audiences inside the repository:

- **Readers of the repository** — people who browse `crates/` and need to know what a crate is for.
- **Contributors** — people who need to understand a crate's role in the Krabka workspace.

## What Belongs in READMEs

- **One-line description** — what the crate does.
- **Role in Krabka** — how it fits into the larger system, and which signal it serves.
- **Key features and capabilities**, including the query language or wire API it covers and the upstream component it must match.
- **Quick start or usage example** (for binaries and public API crates).
- **Configuration reference** (for server binaries).
- **Links** to design docs, test coverage reports, and the differential suite that checks the crate.

## What Does NOT Belong in READMEs

- **Exhaustive API reference** — that belongs in rustdoc.
- **Design rationale** — that belongs in the design doc.
- **Test coverage details** — that belongs in the coverage report.
- **TODO lists or known issues** — those belong in the issue tracker, not in per-crate READMEs.

## Document Structure

### Library Crates

```markdown
# krabka-<name>

<One-line description of what this crate does.>

Part of [krabka-o11y](https://github.com/krabka-io/krabka-o11y), the Krabka observability stack.

## Overview

<2-3 sentences explaining the crate's role in the system, which signal it
serves, which query language or wire format it implements, and its
relationship to other Krabka crates.>

## Features

- Feature 1
- Feature 2
- Cargo feature: `feature-name` — what it enables

## Usage

```rust
// Minimal example showing the primary API
```

## Documentation

- [Design](docs/design.md)
- [Test Coverage](docs/test_coverage_report.md)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
```

Omit a `## Documentation` line when the file it names does not exist. Do not link a document you have not written.

### Server / Binary Crates

```markdown
# krabka-<name>

<One-line description of what this binary does.>

Part of [krabka-o11y](https://github.com/krabka-io/krabka-o11y), the Krabka observability stack.

## Quick Start

<3-5 steps to get running, including the minimal flags and the run command.>

```bash
bazel run //crates/<name>:krabka-<name> -- --help
```

## Configuration

<Table of configuration options with defaults and descriptions.>

Every option is a command-line flag and an environment variable
(`KRABKA_<NAME>_` prefix).

| Option | Default | Description |
|--------|---------|-------------|
| ... | ... | ... |

## Documentation

- [Design](docs/design.md)
- [Test Coverage](docs/test_coverage_report.md)

## License

Apache-2.0. See [LICENSE](../../LICENSE).
```

### Small / Internal Library Crates

For crates under about 200 lines with a single responsibility:

```markdown
# krabka-<name>

<One-line description.>

Part of [krabka-o11y](https://github.com/krabka-io/krabka-o11y), the Krabka observability stack.
<1-2 sentences on what it does and which crate(s) use it.>

## License

Apache-2.0. See [LICENSE](../../LICENSE).
```

## Writing Style

- **Be concise** — READMEs should be scannable. If a section exceeds a screenful, it probably belongs in a separate doc.
- **Lead with the most useful information** — what it does, not how it is built.
- **Use concrete examples** — a 5-line code snippet is worth a paragraph of description.
- **Link, do not duplicate** — point to the rustdoc for API details, the design doc for rationale, the coverage report for what is tested, and the root [`README.md`](../../README.md) for the differential suites.
- **State the compatibility scope honestly** — say which part of the upstream surface the crate covers, and name the suite or corpus that establishes it. Do not imply full compatibility that no test checks.

## Badges

Krabka crate READMEs carry no badges. The crates are not published, so a crates.io or docs.rs badge would link to a crate that this repository does not own. Avoid decorative images too. Prefer text descriptions.

## Naming Conventions

- **Title**: use the crate name as-is (for example, `# krabka-promql`, not `# PromQL Query Engine`).
- **Links**: use relative paths within the repo (for example, `../../LICENSE`, `../../README.md`), not absolute URLs, except for external sites such as the Prometheus, Loki, Tempo, and Pyroscope documentation.
- **License**: American spelling (`## License`), Apache-2.0, and a link to `LICENSE`. Where a crate vendors third-party test data, name the source in the attribution file next to that data, as `crates/promql/tests/testdata/ATTRIBUTION.md` does.

## Questions to Ask When Writing

1. Could someone understand what this crate does from the first two sentences?
2. Is there enough information to use the crate without the source code?
3. Do I duplicate content that lives in another document, such as rustdoc, a design doc, or a coverage report?
4. Would this help someone who opens the crate directory for the first time?
5. Is the compatibility scope stated accurately, and does a test or a differential suite establish every claim?
