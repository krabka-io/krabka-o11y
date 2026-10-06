# Query-language qualification

Krabka cannot currently substantiate full compatibility with all upstream query languages. Qualification must name the upstream revision, feature flags, fixture and oracle hashes, and final Krabka commit. Passing a finite corpus establishes that corpus's contract; it does not prove correctness for every possible query or dataset.

## Current executable coverage

The pinned oracle images are Prometheus 3.14.0, Mimir 3.2.1, Loki 3.7.7, Tempo 3.0.3 and Pyroscope 2.3.1. [The image manifest](../bazel/images/images.bzl) records their immutable identities. [Source notes](query_language_suite_sources.md) explain runner contracts, licences and adaptations.

| Surface | Runnable qualification | Limits retained in evidence |
| :--- | :--- | :--- |
| PromQL | The separate [Prometheus 3.14 corpus](../crates/promql/tests/testdata/upstream-3.14.0/ATTRIBUTION.md) contains 21 files and 2,195 evaluation cases, including fill modifiers and start timestamps. `//crates/promql:upstream_qualification_test` and its `_experimental_test` variant attempt every case and emit individual verdicts. | The experimental configuration matches all 2,195 cases locally, including expected errors and annotations. The default configuration reports 2,078 matches and 117 source-verified feature-disabled cases, with zero mismatches. No local expected-divergence annotations alter this corpus. These execution-completeness targets retain every verdict; the aggregate qualification gate checks compatibility separately. Historical regression targets remain separate. |
| Public PromQL APIs | [Prometheus](../crates/metrics-service/tests/diff_prometheus.rs) runs the pinned compliance catalogue on identically seeded HTTP backends; [Mimir](../crates/metrics-service/tests/diff_mimir.rs) remains a separate oracle. | The external compliance runner compares range queries, ignores warnings and accepts paired errors in some negative cases. Agreement with Prometheus does not certify Mimir's fork or all API error contracts. |
| LogQL | [Loki differential tests](../crates/observability/tests/loki_differential.rs) run the pinned upstream remote correctness runner in instant and range modes, with seeded data and explicit expanded-case reports. | Catalogue adaptations and skips remain visible. A catalogue pass does not cover every grammar production, native histogram payload or pagination contract. |
| TraceQL | [Tempo differential tests](../crates/traces/tests/tempo_differential.rs) compare independent OTLP forest fixtures, exact selected trace/span identities, and metric labels, timestamps and values. Comparator negative controls detect changed identities and cancelling values. | The golden corpus and live fixtures cover different contracts. Separate live artifacts check numeric metrics, typed grouping, singleton exemplars and field arithmetic against independent expected values. Dynamic field comparisons retain their generated case ledger. Multi-trace exemplar sampling, array arithmetic and some scoped operands remain gaps. |
| Profile queries | [Pyroscope differential tests](../crates/profiles/tests/pyroscope_differential.rs) exercise populated span-profile and query-analysis requests through JSON and binary Connect, with exact normalized stacks and independent sample counts. | The architecture-specific matrix covers all 12 RPCs with populated profiles, v2 heatmaps, grouping, stack prefixes, bounded trees, diff responses, async submission/polling and malformed-input controls. V1 `SelectHeatmap` is unimplemented upstream; paired v2 span-exemplar errors are rejection evidence. Physical byte statistics and distributed async leases remain explicit gaps. See the [request matrix](query_language_profile_matrix.md) for field-specific comparison contracts. |
| Storage transitions | [Backup/restore](../crates/integration/tests/backup_restore.rs) compares query ledgers before the sealed deployment cut and after restore through the production ingestion, WAL and block paths. | Metrics v1/v2 and Tempo deployments also check independent compound query results across hot-only, overlapping, persisted and restarted storage, with tenant negatives (26 comparisons per metrics format, 46 structural TraceQL comparisons and 51 tenant TraceQL comparisons). Additional real block-store ledgers check duplicate metrics through publication, compaction, erasure and index reload, and typed TraceQL comparisons through replacement publication, input deletion and reload. Broader distributed planner and cross-signal erasure compositions remain open. |

The [pinned feature inventory](../qualification/query-language-inventory.json) retains **1,299 entries**: 443 PromQL, 373 LogQL, 369 TraceQL and 114 profile-query features. The [reviewed registry](../qualification/query-language-evidence.json) binds 183 exact executions to 180 features. Each binding records source hashes, request and configuration identities, typed expressions and independent ledgers. Current mappings are 50 fully mapped, 130 partially mapped and 1,119 uncovered features. Entries without complete positive, negative and composition evidence remain explicit qualification gaps. Execution counts never replace this denominator.

[The shared generated runner](../crates/metrics-service/tests/support/generated_differential.rs) builds typed expression trees for all four surfaces with seed 42 and depth at most three. It defaults to 16 cases; nightly requests 512 with `KRABKA_GENERATED_NIGHTLY=1`. Result types, operators, depths and type-preserving shrink parents accompany each execution. Separate invalid-query generators require explicit parser/type rejection from both servers. Rejections cannot satisfy positive or composition mappings. The generators cover bounded families, not every upstream grammar production.

## Running and retaining evidence

[The query-conformance workflow](../.github/workflows/query-conformance.yml) runs nightly and on demand. Each Docker suite gets a separate runner; default and experimental full PromQL reports, profile deployment transitions and backup/restore also run separately. The workflow reuses Bazel setup, remote caching and the pinned image wrappers. Prometheus and Loki launch through `tools/build-query-runners.py --bazel`, which records upstream source revision, toolchain, adaptation and binary hashes.

Raw outputs, test logs, runner profiles, oracle-image pins and SHA-256 manifests are uploaded even when a suite fails, under artifact names containing the final commit and run attempt. The downstream job verifies artifact hashes and aggregates their verdicts. Evidence downloads live outside the source checkout.

For a local aggregate after collecting the suites' undeclared outputs:

```sh
python3 tools/query-language-inventory.py --check
python3 tools/query-language-report.py \
  --evidence /path/to/collected-evidence \
  --output /path/to/query-language-qualification.json
```

The report binds the checkout SHA, inventory, fixtures and image manifest to collected evidence. Missing suites, failed run results, mismatches, expected divergences, unsupported cases or uncovered inventory entries keep `complete_versioned_conformance` false. PR jobs retain checksummed evidence bound to their checkout and validate every selected query report. Nightly aggregation also requires every suite and rejects invalid source hashes, configurations, runner bindings, execution denominators and failed results. Declared feature gaps and expected upstream differences remain visible; the manual workflow's `strict` input additionally requires complete versioned conformance. A green execution and provenance gate is not a complete compatibility verdict.

## Next qualification work

The machine inventory is the feature-level gap ledger. Fully mapped features still need exact successful execution artifacts before qualification; mismatches, missing evidence and unsupported oracle cases remain in their denominators.

Broader work includes distributed storage/tenant-erasure compositions, every remaining grammar production and RPC field combination, multi-trace exemplar sampling, and real Grafana/client request replay. PromQL evaluation supports histogram trim operators. The dependency's generic AST formatter and serializer do not know their private token IDs. Formatter round trips are not qualified. Arbitrary invalid UTF-8 string bytes also remain outside the Rust string representation, although invalid label-name rejection is qualified by the corpus.

Only publish a versioned corpus claim when its recorded denominator contains matches alone, with no missing suites or uncovered advertised features. Local results and workflow configuration do not establish remote success on the final pushed SHA.
