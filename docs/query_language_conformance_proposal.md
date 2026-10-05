# Query-language qualification

Krabka cannot currently substantiate full compatibility with all upstream query languages. Qualification must name the upstream revision, feature flags, fixture and oracle hashes, and final Krabka commit. Passing a finite corpus establishes that corpus's contract; it does not prove correctness for every possible query or dataset.

## Current executable coverage

The pinned oracle images are Prometheus 3.14.0, Mimir 3.2.1, Loki 3.7.7, Tempo 3.0.3 and Pyroscope 2.3.1. [The image manifest](../bazel/images/images.bzl) records their immutable identities. [Source notes](query_language_suite_sources.md) explain runner contracts, licences and adaptations.

| Surface | Runnable qualification | Limits retained in evidence |
| :--- | :--- | :--- |
| PromQL | The separate [Prometheus 3.14 corpus](../crates/promql/tests/testdata/upstream-3.14.0/ATTRIBUTION.md) contains 21 files and 2,195 evaluation cases, including fill modifiers and start timestamps. `//crates/promql:upstream_qualification_test` and its `_experimental_test` variant attempt every case and emit individual verdicts. | These targets gate execution completeness, not zero mismatches. This corpus carries no local expected-divergence annotations; consult each fresh report for compatibility counts. Historical 3.8 regression targets remain separate. Annotation/warning fidelity and configuration-specific feature support need explicit qualification. |
| Public PromQL APIs | [Prometheus](../crates/metrics-service/tests/diff_prometheus.rs) runs the pinned compliance catalogue on identically seeded HTTP backends; [Mimir](../crates/metrics-service/tests/diff_mimir.rs) remains a separate oracle. | The external compliance runner compares range queries, ignores warnings and accepts paired errors in some negative cases. Agreement with Prometheus does not certify Mimir's fork or all API error contracts. |
| LogQL | [Loki differential tests](../crates/observability/tests/loki_differential.rs) run the pinned upstream remote correctness runner in instant and range modes, with seeded data and explicit expanded-case reports. | Catalogue adaptations and skips remain visible. A catalogue pass does not cover every grammar production, native histogram payload or pagination contract. |
| TraceQL | [Tempo differential tests](../crates/traces/tests/tempo_differential.rs) compare independent OTLP forest fixtures, exact selected trace/span identities, and metric labels, timestamps and values. Comparator negative controls detect changed identities and cancelling values. | The golden corpus and live fixtures cover different contracts. Count/rate probes explicitly disable exemplars; full live exemplar, type and metrics-function coverage remains open. |
| Profile queries | [Pyroscope differential tests](../crates/profiles/tests/pyroscope_differential.rs) exercise populated span-profile and query-analysis requests through JSON and binary Connect, with exact normalized stacks and independent sample counts. | The v1 populated-RPC fixture records 84 matched comparisons, four expected divergences and one unsupported oracle case. Time-excluded `AnalyzeQuery` deliberately checks upstream count 1 versus Krabka count 0. V1 `SelectHeatmap` is unimplemented upstream; v2 heatmap values, groups and exemplars remain a gap. The complete 12-RPC request-field matrix is still incomplete. |
| Storage transitions | [Backup/restore](../crates/integration/tests/backup_restore.rs) compares query ledgers before the sealed deployment cut and after restore through the production ingestion, WAL and block paths. | Metrics v1/v2 and Tempo deployments also check independent compound query results across hot-only, overlapping, persisted and restarted storage, with tenant negatives (26 comparisons per metrics format, 46 structural TraceQL comparisons and 51 tenant TraceQL comparisons). Broader compaction, distributed planner and tenant-erasure compositions remain open. |

The [pinned feature inventory](../qualification/query-language-inventory.json) currently contains **1,299 uncovered entries**: 443 PromQL, 373 LogQL, 369 TraceQL and 114 profile-query entries. Entries include grammar productions, functions and RPC/request fields; they are not interchangeable with evaluation-case counts. Executing a test does not automatically mark an inventory entry covered. Map each feature to positive, negative and composition evidence, or retain an explicit gap.

[The shared generated runner](../crates/metrics-service/tests/support/generated_differential.rs) composes valid fixture expressions for all four surfaces with seed 42 and depth at most three. It defaults to 16 cases for short runs. Nightly runs request 512 with `KRABKA_GENERATED_NIGHTLY=1`; semantic mismatches shrink through parent expressions and preserve attempted reductions. These bounded templates supplement the corpus; all four suites also generate invalid expressions and require explicit parser/type rejection from both servers. Rejection evidence cannot satisfy positive or composition feature mappings. A complete typed AST generator remains future work.

## Running and retaining evidence

[The query-conformance workflow](../.github/workflows/query-conformance.yml) runs nightly and on demand. Each Docker suite gets a separate runner; default and experimental full PromQL reports and backup/restore also run separately. The workflow reuses Bazel setup, remote caching and the pinned image wrappers. Prometheus and Loki launch through `tools/build-query-runners.py --bazel`, which records upstream source revision, toolchain, adaptation and binary hashes.

Raw outputs, test logs, runner profiles, oracle-image pins and SHA-256 manifests are uploaded even when a suite fails, under artifact names containing the final commit and run attempt. The downstream job verifies artifact hashes and aggregates their verdicts. Evidence downloads live outside the source checkout.

For a local aggregate after collecting the suites' undeclared outputs:

```sh
python3 tools/query-language-inventory.py --check
python3 tools/query-language-report.py \
  --evidence /path/to/collected-evidence \
  --output /path/to/query-language-qualification.json
```

The report binds the checkout SHA, inventory, fixtures and image manifest to collected evidence. Missing suites, failed run results, mismatches, expected divergences, unsupported cases or uncovered inventory entries keep `complete_versioned_conformance` false. Nightly aggregation reports these gaps without applying `--strict`; the manual workflow's `strict` input adds that gate. Ordinary suite failures still fail their jobs. A green execution/reporting job is not a complete compatibility verdict.

## Next qualification work

1. Link every inventory entry to executable evidence with a balanced denominator: discovered = matched + mismatched + skipped + unsupported + expected divergence + uncovered. Preserve failing cases and configuration differences rather than annotating them away.
2. Resolve the full 3.14 PromQL report's mismatches, then qualify warning/error codes, native histograms, sparse data, NaN/infinities, resets, UTF-8 labels and boundary timestamps. Retain the old regression corpus independently.
3. Extend LogQL and TraceQL typed compositions and their generated invalid-query cases. Add exact grouped metric/exemplar comparisons, missing attributes, partial trees and end cutoffs using pinned upstream behavioural fixtures and independent expected results.
4. Complete populated positive and negative coverage for every Pyroscope RPC request field, including profile/trace/span IDs, stack selectors, grouping and time bounds. Add a separately configured v2 Heatmap oracle instead of treating v1 rejection as a semantic pass.
5. Reuse the query ledger across hot/cold overlap, compaction, restart, split ranges and multiple shards. Add overlapping tenant identifiers and deletion before/after restore; preserve multiplicity for independent equal ingests.
6. Grow deterministic typed generators and shrink failures into permanent regressions. Replay real Grafana/client requests as a separate API contract, and use comparator negative controls to demonstrate that the harness rejects wrong answers.

Only publish a versioned corpus claim when its recorded denominator contains matches alone, with no missing suites or uncovered advertised features. Local results and workflow configuration do not establish remote success on the final pushed SHA.
