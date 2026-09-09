# Test Coverage Report Style Guide

This guide defines the style and content expectations for per-crate `test_coverage_report.md` documents in Krabka. The [prose style guide](prose_style_guide.md) defines the wording rules that apply to everything you write here.

## Purpose

Coverage reports capture the **current state of test verification** against Krabka's compatibility contract. They answer: "what is tested, how, and what remains?" They are for reviewers who assess upstream compatibility and for engineers who plan test work.

Coverage reports are **living documents**. Update a report when you add tests. A coverage report is not a frozen snapshot. The date in the header shows the last update.

## Krabka's Verification Model

Krabka has no formal requirements database, and it keeps no hand-maintained compatibility matrix. Its "requirements" are the **upstream compatibility contract**: the query language, the HTTP API, and the wire format that Prometheus, Grafana Mimir, Grafana Loki, Grafana Tempo, and Grafana Pyroscope define for each signal. Verification traces to two kinds of executable artifact, and to nothing else:

- The **six differential suites** listed in the root [`README.md`](../../README.md#differential-suites). Each one boots the upstream component from an image pinned by digest in [`MODULE.bazel`](../../MODULE.bazel) and compares Krabka against it.
- The **vendored conformance corpora**: the Prometheus `promql` corpus under [`crates/promql/tests/testdata/`](../../crates/promql/tests/testdata/ATTRIBUTION.md), which names the upstream tag each file came from, and the TraceQL golden corpus under `crates/traceql/tests/testdata/traceql/`.

Neither artifact is a document that someone can update out of step with the code. That is the point. A compatibility claim that no suite and no corpus establishes does not belong in a coverage report.

A coverage report shows how the tests verify that contract for the surface a crate owns. The methods are unit tests, property tests (round-trip and invariants), file-backed corpora, differential tests against a live upstream oracle, mutation testing, and line coverage.

## What Belongs in Coverage Reports

- **Compatibility coverage** — which upstream behaviours the crate owns are verified, and which tests verify them.
- **Test inventory** — what tests exist, where they are, what they cover.
- **Coverage metrics** — line coverage from `cargo llvm-cov`, and mutation results from `cargo mutants`.
- **Gap analysis** — what is not yet tested and why.
- **Cross-references** — tests in other crates (or the differential suite) that verify behaviour this crate owns.

## What Does NOT Belong in Coverage Reports

- **Design rationale** — how or why the code works; the design doc does that.
- **Bug reports or change history** — coverage reports track the current state, not the past changes.
- **Aspirational targets** — report what IS covered, not what coverage SHOULD be.
- **Duplicated upstream semantics** — link the upstream documentation. Do not restate what a `PromQL` function or a TraceQL operator means.

## Document Structure

Every coverage report should follow this structure. You may omit a section that is genuinely not applicable, for example when the crate has no fuzz targets, but you should keep the order.

```markdown
# <crate-name> Test Coverage Report

| Document Info | Details |
| :--- | :--- |
| **Crate** | `krabka-<name>` |
| **Signal** | <metrics / logs / traces / profiles, or "all" for the block store> |
| **Upstream surface** | <query language, HTTP API, or wire format this crate owns> |
| **Date** | <YYYY-MM-DD of last update> |
```

### Section 1: Compatibility Coverage Summary

Lead with a one-line summary: "All owned behaviours verified (N differential, M corpus, K unit)" or "N of M behaviours verified".

Include a table that maps each upstream behaviour the crate owns to its verification status and to the test that establishes it.

```markdown
| Surface | Behaviour | Result | Test | Oracle |
| :--- | :--- | :--- | :--- | :--- |
| **PromQL** | Counter-reset handling in `rate` | Pass | `functions.test`, run by `conformance.rs` | Prometheus corpus at `v3.5.0` |
| **HTTP API** | `/api/v1/query_range` response shape | Pass | `diff_prometheus` scenario `range_query` | `prom/prometheus` image |
```

- **Every behaviour the crate owns must appear** — even if the result is `N/A` or `Not tested`.
- **Result values**: `Pass` (test exists and passes), `Fail` (test exists and fails), `N/A` (not applicable to this crate), `Not tested` (no test exists).
- **Test column**: cite specific test function names (`file::function`), not just file paths. For corpus coverage, name the corpus file. For differential coverage, name the suite and the scenario.
- **Oracle column**: name what establishes the expected result. That is the pinned upstream image for a differential row, the corpus file and its upstream tag for a conformance row, and `hand-written` for a behaviour that only this repository defines.
- **Cross-crate or differential coverage**: when tests elsewhere verify the behaviour, say `Pass (differential)` or `Pass (krabka-traces)` and cite the specific test.

### Section 2: Test Inventory

List all tests, grouped by type. Include a total count per group.

- **Unit tests**: table with columns `Test Function | File | Scope`.
- **Property tests**: `proptest` and `datatest-stable` corpora — `Test Function | File | Property`.
- **Integration tests**: separate subsection, test file path and suite breakdown (note which are `#[ignore]`d and need Docker and `testcontainers`).
- **Differential tests**: the upstream-oracle scenarios that check this crate's behaviour, and the image each suite boots.
- **Fuzz tests** (if any): `Target | File | Status`.

When a crate has no unit tests, state "No unit tests are implemented" and explain why. The reason can be a thin wrapper, logic that differential tests verify end-to-end, or backend-specific scenarios that would only test a third-party library's semantics. Do not manufacture coverage that does not exist.

### Section 3: Coverage vs Scope

Include a table that cross-references the crate's intended scope, from its design doc and its upstream surface, against the implementation status:

```markdown
| Area | Scenario | Planned | Implemented | Status |
```

- **Status**: `Complete`, `N/M remaining`, or `Delegated to <crate>`.
- **Delegated tests**: when another crate or the differential suite covers a scenario, use `—` for counts and explain in Status. Adjust the totals to match.
- **Include a total row** with a percentage.

### Section 4: Line Coverage

CI measures line coverage with a separate `bazel coverage` job over every crate. It is expensive and it evicts the normal build cache, so run the scoped `cargo llvm-cov` equivalent over the `nextest` runner instead:

```bash
cargo llvm-cov nextest --package <crate> --profile ci --lib --bins --lcov --output-path lcov.info
lcov --summary lcov.info
```

The container suites are `#[ignore]`d, and they verify most of the behaviour in some crates. For those crates, add the run that includes them. It needs a Docker daemon:

```bash
cargo llvm-cov nextest --package <crate> --profile ci --lcov --output-path integration.lcov -- --ignored --nocapture
```

- **Quote the summary output** in a code block.
- **Give a per-file breakdown** as a table: `File | Covered | Total | Coverage | Notes`.
- **Sort by coverage descending** — readers see strengths first, gaps last.
- **Note what the numbers exclude**: "unit tests only" or "includes ignored integration tests".
- **Explain anomalies**: generated prost types that inflate counts, `Display` impls at 0%, generic monomorphisation, and so on.

If you have not yet measured line coverage, include the command block so the reader can run it. Do not write "has not been run". Give the command and leave space for results. If line coverage is genuinely not applicable, state why and omit the section. One example is a thin wrapper that an external harness verifies entirely.

### Section 5: Mutation Coverage

Krabka gates on **mutation testing** (`cargo mutants`). Mutation testing is a stronger signal than line coverage. A surviving mutant means that the line runs but that no test asserts on its behaviour.

```bash
git diff origin/main...HEAD | tee git.diff
cargo mutants --in-diff git.diff
```

The full sweep is a Bazel target per crate, sharded, and it runs from the scheduled job rather than on a pull request:

```bash
bazel test //crates/<name>:<name>_mutants
```

- Report **surviving mutants** as gaps. Each one is a line whose behaviour no test checks.
- **Quote a survivor count only when the totals line adds up.** A shard that overruns its bound reports nothing rather than failing, so `caught + missed + unviable == total` is the check that the sweep finished.
- A crate with zero surviving mutants over its changed surface is the target state. Say so when this is true.

### Section 6: Test Infrastructure

Describe briefly how you structure the tests. This section is optional for simple crates. Cover these points when they apply:

- Test helpers and fixtures, and where they live.
- `assert2` (`assert!` and `check!`) for assertions — the workspace standard.
- Trait seams and in-memory fakes that isolate IO-decision logic from a live store.
- `datatest-stable` harnesses, and which corpus directory each one walks.
- `testcontainers` and the upstream differential oracle for interop tests, and how to run them (`--include-ignored`, or `bazel test --config=docker`).
- Runtime requirements, for example `multi_thread` for concurrent tests.

### Section 7: Key Gaps

A table of remaining coverage gaps:

```markdown
| Area | Gap | Severity | Notes |
```

- **Only list genuine gaps** — not items that another crate or the differential suite covers.
- **Severity**: High (an upstream-compatibility behaviour is unverified), Medium (a significant code path is untested), Low (edge case or defence-in-depth).
- **If no gaps remain**, state it explicitly: "All owned behaviours verified. No significant gaps remain."

### Section 8: Conclusion

Write a single paragraph that summarises:

- Total test count and scope-coverage percentage.
- Line coverage percentage and mutation-testing status.
- How many owned upstream behaviours are verified. This is the headline metric.
- Key strengths, which are the well-tested areas.
- Primary remaining gaps, if there are any.
- Other test layers that contribute: differential, property, fuzz, and CLI.

## Cross-Crate and Differential References

When a crate delegates verification elsewhere:

- **In the coverage summary**: use `Pass (differential)` or `Pass (<crate>)` and cite the specific test or scenario.
- **In Coverage vs Scope**: use `Delegated to <crate>` and adjust totals to show "in-scope" vs "total".
- **Do not double-count**: if a differential scenario or a test in crate A verifies behaviour that crate B owns, only crate B's report counts it as verified. Crate A notes the test but does not inflate its own counts.

## Consistency Rules

- **Header table**: always use `| :--- | :--- |` alignment.
- **Date format**: `YYYY-MM-DD`.
- **Test function names**: use `file::function_name` format (for example, `parser.rs::rate_over_counter_reset`).
- **Corpus references**: name the file (for example, `functions.test`) and the harness that runs it.
- **Percentages**: one decimal place for line coverage (for example, 78.2%), zero for scope coverage (for example, 92%).
- **File paths**: relative to crate `src/` for source, relative to crate root for test files.

## Root-Level Coverage Report

The project-level report at `docs/test_coverage_report.md` summarises all crates. It should:

- List all crates with their coverage report and status.
- Aggregate test counts (unit, property, integration, differential, fuzz).
- Summarise upstream-compatibility coverage per signal.
- Identify cross-cutting gaps.
- Link to per-crate reports rather than duplicate their content.

## Requirements Traceability

Krabka's verification chain runs from the upstream compatibility contract to individual test results:

```
Upstream compatibility contract
  (query language + HTTP API + wire format, per signal)
        │
        ├── differential suites vs a live upstream oracle   ← authoritative
        │     (image pinned by digest in MODULE.bazel)
        ├── vendored conformance corpora                    ← authoritative
        │     (Prometheus promql corpus, TraceQL golden corpus)
        ├── property tests (round-trip / invariants) + fuzz
        └── unit / integration tests
              │
              └── Individual test functions → Pass/Fail
                    (mutation-verified, line-coverage measured)
```

Each coverage report's Section 1 table connects an upstream behaviour to the test result that establishes it. The `Oracle` column makes that traceability explicit. The root-level report aggregates these per signal.

## Questions to Ask When Writing

1. Does the summary table account for every upstream behaviour the crate owns?
2. Does the test inventory match what `cargo nextest run` runs, including the ignored container suites?
3. Are cross-crate and differential references accurate and up to date?
4. Would a reviewer know exactly what is and what is not verified against the upstream component?
5. Are the line-coverage numbers current, and does the report list surviving mutants as gaps?
