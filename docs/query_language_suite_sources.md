# Query-language suite implementation sources

Source inspection on 2026-10-06. Commands below require running reference and Krabka endpoints with identical seeded data; they are setup instructions, not passing-test evidence. Downloaded source excerpts, original licences, a SHA-256 manifest, the complete Loki YAML catalogue, and independent expected-value examples are under `/home/matt/.codex/tmp/query-conformance-research/`.

| Source | Pinned revision | Module/toolchain | Repository licence |
| :--- | :--- | :--- | :--- |
| Prometheus compliance | `67b8327a2e93dc28f64d4b21bbce00b362f565d5` | `promql/go.mod`: Go 1.25.0 | [Apache-2.0](https://github.com/prometheus/compliance/blob/67b8327a2e93dc28f64d4b21bbce00b362f565d5/LICENSE); `testcases/expand.go` also carries an InfluxData MIT notice |
| Loki 3.7.7 | `7a40404f32b3e6464c9cfc6cc7dd75a40f3931da` | root `go.mod`: Go 1.26.5 | [AGPL-3.0](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/LICENSE) |
| Tempo 3.0.3 | `1900ed7bb5cad1a3edc285783d7d4ac4278337dc` | Behavioural fixture source; not a Rust harness | [AGPL-3.0](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/LICENSE) |
| Pyroscope 2.3.1 | `7aeaa0ff91e83538b3ff0d09bfefb168bddc022d` | Behavioural fixture source; not a Rust harness | [AGPL-3.0](https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/LICENSE) |

Preserve source revision, path, licence, copyright notices, and adaptation details for material copied into fixtures. The current Krabka repository licence is Apache-2.0; keep upstream runner checkouts and their licences explicit rather than treating downloaded Go source as locally authored code. Independently authored input/output fixtures can record their behavioural reference without copying an upstream implementation.

## Prometheus compliance runner

Build from the pinned compliance checkout's **`promql/` subdirectory**, which is its own module:

```sh
GOTOOLCHAIN=go1.25.0 go build -o /tmp/promql-compliance-tester ./cmd/promql-compliance-tester
/tmp/promql-compliance-tester \
  -config-file=/fixtures/targets.yml \
  -config-file=./promql-test-queries.yml \
  -output-format=json -output-passing -query-parallelism=4 > /artifacts/compliance.json
```

[`config/config.go`](https://github.com/prometheus/compliance/blob/67b8327a2e93dc28f64d4b21bbce00b362f565d5/promql/config/config.go) defines this target file:

```yaml
reference_target_config:
  query_url: http://prometheus:9090
test_target_config:
  query_url: http://krabka:9090
  headers:
    X-Scope-OrgID: conformance
query_time_parameters:
  end_time: "2026-10-05T00:00:00Z"
  range_in_seconds: 600
  resolution_in_seconds: 10
```

Choose `end_time` from the fixture anchor, not this example date. Pre-validate it: [main.go](https://github.com/prometheus/compliance/blob/67b8327a2e93dc28f64d4b21bbce00b362f565d5/promql/cmd/promql-compliance-tester/main.go) silently falls back to `now - 12m` for invalid/missing time. Zero range/resolution also selects defaults. Repeated config files are concatenated before strict YAML decoding, so avoid duplicate top-level keys.

The [query catalogue](https://github.com/prometheus/compliance/blob/67b8327a2e93dc28f64d4b21bbce00b362f565d5/promql/promql-test-queries.yml) expects `demo_memory_usage_bytes`, `demo_num_cpus`, `demo_cpu_usage_seconds_total`, `demo_disk_usage_bytes`, `demo_batch_last_success_timestamp_seconds`, `demo_intermittent_metric`, and classic `demo_api_request_duration_seconds_bucket` series. Retain `job="demo"`, `instance="demo.promlabs.com:10000|10001|10002"`, and memory `type` labels because literal selectors/regexes refer to them.

For a bounded adapter, seed deterministic samples every 5s from at least **80 minutes before the query end through 10 minutes after it**. The [expander](https://github.com/prometheus/compliance/blob/67b8327a2e93dc28f64d4b21bbce00b362f565d5/promql/testcases/expand.go) includes 1h ranges and positive/negative 10m offsets. This is approximately 1,081 samples per series, not an hour of wall-clock waiting. Feed both APIs the exact same remote-write payload; configure historical/future ingestion bounds to admit the selected anchor. Include resets, gaps/stale markers, multiple memory types, and monotonic cumulative histogram buckets including `+Inf`; validate nonempty representative selectors before comparison.

The [comparer](https://github.com/prometheus/compliance/blob/67b8327a2e93dc28f64d4b21bbce00b362f565d5/promql/comparer/comparer.go) executes **range queries only**, ignores warnings, and accepts any paired errors for `should_fail`. Default float tolerance is relative `1e-5` with zero absolute margin. Cases marked `skip_comparison` bypass payload equality. Reject empty reports, skipped comparisons, unexpected success/failure, and nonzero `unsupported` counts; do not call a paired error the same error contract. Supplement instant queries, warning/error codes, and native histograms in Krabka's own differential runner. The JSON [output](https://github.com/prometheus/compliance/blob/67b8327a2e93dc28f64d4b21bbce00b362f565d5/promql/output/json.go) includes `totalResults`, every result, and `queryTweaks` for a machine-readable gate.

## Loki remote correctness runner

Run from the root of the pinned full Loki checkout, preserving `pkg/logql/bench/queries/`:

```sh
GOTOOLCHAIN=go1.26.5 go test -json -count=1 -tags=remote_correctness \
  ./pkg/logql/bench -run=TestRemoteStorageEquality -timeout=30m \
  -addr-1=http://loki:3100 -addr-2=http://krabka:3101 \
  -org-id=conformance -metadata-dir=/fixtures/loki \
  -seed=42 -remote-range-type=range \
  -remote-include-skipped=true -tolerance=0.00001 > /artifacts/logql-range.jsonl
```

Repeat with `-remote-range-type=instant`. [`remote_test.go`](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/bench/remote_test.go) loads three suites and filters log queries out of instant mode. The catalogue contains **101 definitions: 73 metric, 28 log, 15 marked skipped**; direction expansion changes executable case counts. Report definitions and expanded cases separately. Missing endpoints/metadata flags skip the entire test, so the JSON-event gate must require actual named subtests and reject `skip` events.

[`metadata.go`](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/bench/metadata.go) reads `dataset_metadata.json` with version `"1.0"`. All pinned definitions request a **24h** window; shorter metadata fails expansion. A bounded local dataset can use two streams (JSON and logfmt) at 10s intervals over 24h, roughly 17,282 entries. Provide labels `cluster`, `container`, `namespace`, `pod`, `service_name`; parsed `level`; numeric `bytes`, `duration_ms`, `size`, `status`; valid duration strings; structured `detected_level`; and lines containing `debug`, `duration`, `error`, `failed`, `level`, `refused`. Alternate levels so positive and negative filters both match. Retain fixture records at/before the final instant window and a positive lookback duration.

Populate `all_selectors`, `by_format`, `by_label_key`, `by_keyword`, `by_detected_field`, `by_unwrappable_field`, and `by_structured_metadata` consistently with those records, plus RFC3339 `time_range` and `metadata_by_selector`. Go `time.Duration` values `min_range`/`min_instant_range` are JSON **nanoseconds** (`60000000000` for 1m), not seconds or strings. The [resolver](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/bench/metadata_resolver.go) intersects every requirement before selecting a stream; invented metadata can otherwise produce false coverage or empty answers.

Two upstream caveats need explicit handling. The [basic catalogue](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/bench/queries/fast/basic-selectors.yaml) deliberately includes an empty-result query, while the runner unconditionally requires every result nonempty. An attributed scratch-checkout adaptation should allow equality of empty answers for that named case, and assert independent emptiness. Also, [numeric assertions](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/bench/assertions_test.go) use absolute `1e-5`, compare float points but omit histogram payloads, and enforce exact labels/timestamps. Log queries have limit 1,000, so add pagination/limit tests separately. Do not silently filter incompatible queries or raise the tolerance to mask a mismatch.

## Tempo fixtures with independent expectations

Use [AST execution tests](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/pkg/traceql/ast_execute_test.go) for small input/output tables:

| Fixture/query | Independent expected answer |
| :--- | :--- |
| `{ .foo = .bar }`, both strings `bzz` | Span matches |
| `{ .foo = "bar" }`, only `.fzz="bar"` exists | Span does not match |
| `{ .foo = .bar }`, `.foo="str"`, `.bar=5` | Span does not match |
| `{} \| by(.foo)`, span IDs 1=`a`, 2=`b`, 3=`b` | Groups `a:[1]`, `b:[2,3]` |

The [vParquet5 tests](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/tempodb/encoding/vparquet5/block_traceql_test.go) provide a two-tree structural fixture: ancestor1 contains descendant1a/1b; ancestor2 contains descendant2a/2b, with 2bb beneath 2b, plus disconnected spans. `TestDescendantOf` explicitly expects only 1a/1b when the left side is ancestor1; self does not match. Negation yields 2a/2b for that right-side set; union also includes ancestor1. Translate nested-set coordinates into independently assigned OTLP parent IDs, preserving the forest and expected identities. Reuse `TestChildOf` and `TestSiblingOf` for immediate versus transitive relationships.

`TestBackendBlockSearchTraceQL` uses 250 randomized decoy traces and `fullyPopulatedTestTrace`; for deterministic qualification fix IDs/order, retain positive and negative search tables, and assert exact selected span/trace IDs. Do not port its random decoy generation as the oracle. Metrics [integration cases](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/integration/api/query_range_test.go) add exemplars, nil/type grouping, limits, top/bottom-k, and end cutoffs. Compare full labels, timestamps, and numeric arrays; a series-count assertion alone is insufficient. Keep duration units explicit (TraceQL duration attributes are nanoseconds; API time units vary by field).

Tempo 3.0.3 preserves array attributes in trace retrieval and spanset grouping, but its metric frontend [StaticFromAnyValue decoder](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/pkg/traceql/engine.go#L481) has no `ArrayValue` branch. The frontend therefore merges array metric labels into a nil group. Public query tests retain that behavior, including the full timestamp grid and independently counted samples; they do not establish distinct array metric label identity. Raw engine grouping tests retain typed arrays separately.

## Pyroscope semantic fixtures

[`Test_StackTraceFilter`](https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/phlaredb/symdb/stacktrace_selection_test.go) defines eight unit-valued stacks and literal expected call-site values. Leaf-to-root location sequences are `[baz,bar:1,foo]`, `[bar:2,foo]`, `[baz,foo]`, `[qux]`, `[bar:1]`, `[foo,bar:1]`, `[bar:2]`, `[foo,bar:2]`. Preserve distinct `bar` source-line locations.

| Root-to-leaf selector | Flat | Total | Location flat | Location total |
| :--- | ---: | ---: | ---: | ---: |
| `foo` | 0 | 3 | 2 | 5 |
| `bar` | 2 | 4 | 3 | 6 |
| `foo,bar` | 1 | 2 | 3 | 6 |
| `foo,bar,baz` | 1 | 1 | 2 | 2 |
| `foo,bar,baz,qux` | 0 | 0 | 1 | 1 |

These are upstream call-site selection counters, not interchangeable with a generic merged-profile sum. Query RPC adapters must verify the corresponding observable result against live Pyroscope.

[`select_merge_test.go`](https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/pkg/querier/select_merge_test.go) has overlapping replica profiles at timestamps 1–6. The literal expected kept set contains each timestamp once; its grouped-series fixture expects values 1–6 at those timestamps. This tests replicas of the same profiles, not deduplication of independent equal ingests. The function named `TestSelectMergeStacktracesWithBlockDeduplication` is **empty** and supplies no evidence.

Enumerate all 12 methods and request fields from the pinned [QuerierService proto](https://github.com/grafana/pyroscope/blob/7aeaa0ff91e83538b3ff0d09bfefb168bddc022d/api/querier/v1/querier.proto), including profile/trace/span identifiers, stack selectors, heatmaps, analysis, and deprecated methods. Test binary and JSON Connect transport, selector escaping/missing labels, time endpoints, grouping, and independent repeated ingests. Normalize profile results by function/source-line stacks and sample values, preserving profile type, units, and multiplicity; serialization order or dictionary IDs are not stable expected values.

## Supported template functions and Go semantics

The pinned Loki [formatter function map](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/log/fmt.go) registers an explicit subset of Sprig plus Loki helpers and Go template builtins. Uppercase legacy helper aliases marked deprecated are retained in historical evidence. New coverage enumerates the supported registrations and independently checks typed truthiness, lazy operand selection, arithmetic, JSON collection values, variable reassignment, named templates, range control and runtime-error filtering. The [shared function fixture](../crates/logql/tests/support/template_functions.rs) runs both through the Rust renderer and paired live Loki HTTP requests. Numeric edge cases preserve integer comparison precision, wrapping arithmetic, Go JSON float64 decoding, negative rounding and scientific notation. Template runtime errors keep the original line and expose `TemplateFormatErr` for pipeline filtering.

Go template strings retain arbitrary bytes until the HTTP JSON boundary. Rune constants, string slicing, regex captures, URL escaping and printf have independent byte and type controls. Named templates use heap continuations and the pinned Go execution-depth limit; range assignment and nested break/continue retain lexical scopes. Time formatting uses Go 1.26.5's pinned timezone archive; an independent 4,784-observation ledger checks all 598 zones at eight instants, including historical and far-future transitions. [Timezone provenance](../crates/logql/src/template/template_time/TIMEZONE-DATA.md) records the data hashes and licences.

Trace retrieval fixtures retain scalar and array types, empty and singleton arrays, empty OTLP values, byte strings, nested arrays, key-value lists and non-finite numbers at span, event and link locations. Both JSON and protobuf responses are compared with the same independent OTLP ledger. The live oracle enables vParquet5 for its typed intrinsic storage contract. Tempo's pinned SeriesSet protobuf encoder omits `promLabels`; that omission is recorded while independent candidate controls check the complete label spelling and association.

Prometheus rule templates use a separate binding of the [3.14.0 expander](https://github.com/prometheus/prometheus/blob/d7598b7141418fa35be2b5ec5d0fefb634199610/template/template.go): 29 explicit helpers and 19 Go builtins. The [native source goldens](../crates/logql/tests/support/prometheus_template_functions.rs) retain 55 actual outputs and rejections, including typed time and duration pointers, humanizers, URL helpers, sample identity and shared query-result sorting. Runtime queries resolve in execution order at the fixed rule timestamp, including queries built from labels or earlier results. Scalar results become one sample; histogram samples retain typed fields and buckets. Matrix and string results are execution errors. Independent ruler and HTTP ledgers check histogram alert values and preserve query order across render retries. Sorting shares slice backing within one render, including during range iteration; each retry starts with the original query result order. These source tests do not supply the missing positive, negative and composition artifact bindings for every new inventory entry.


## Experimental queries supported by the pinned engines

Experimental features remain in scope when upstream supports them without deprecation. Loki's [multi-variant evaluator](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/evaluator.go) uses the common `of` selector and scan window, each variant's own evaluation window, and source-specific extractor composition. Its [result assembler](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/engine.go) applies a separate series budget per variant and retains warnings when a variant is discarded. Instant and range outputs differ when an aggregation removes `__variant__`. The default `enable_multi_variant_queries` gate remains disabled; tenant overrides enable it explicitly. The native frontend reports HTTP 400 for disabled variants. Common structured metadata re-enters the variant extractor before aggregation, with source collision names and category changes preserved. Federated variant plans retain physical selector matchers; a common `__tenant_id__` matcher against streams without that physical label returns an empty result.

Loki's [approximate top-k evaluator](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/logql/count_min_sketch.go) uses a count-min sketch and bounded label heap, including for unsharded queries. Krabka uses the pinned sketch dimensions and hash functions rather than substituting exact top-k. Independent Go goldens check the hash positions, forced collisions and repeated label counts. `shard_aggregations: [approx_topk]` enables the feature. The native oracle uses `frontend.encoding: protobuf` to preserve the internal sketch query plan through the [frontend encoder](https://github.com/grafana/loki/blob/7a40404f32b3e6464c9cfc6cc7dd75a40f3931da/pkg/lokifrontend/frontend/v2/frontend.go). Source-rejected controls retain zero limits, vector-only inputs and range requests with their measured status codes and diagnostics. The native [experimental query fixture](../crates/observability/tests/support/loki_experimental_queries.rs) retains configured requests, raw exchanges, complete normalized cross-server comparisons, independent samples and warnings, and exact disabled-feature diagnostics. Its 40 cases include federated requests: tenant labels enter before aggregation, the variant gate accepts any enabled tenant, the series cap takes the minimum tenant limit, and approximate top-k runs after tenant inputs are merged. Grouped exact top/bottom-k retain full selected labels while ranking within their source grouping. Detected log levels are entry metadata added during ingestion, with upstream field precedence, bounded JSON depth and tenant discovery controls; query execution does not manufacture missing metadata. Four additional native controls cover disabled discovery, custom fields, bounded depth and unlimited depth. JSON extraction retains physical key order, duplicate-key behavior and raw numeric values, with independent parser controls.

Tempo's [metrics pipeline](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/pkg/traceql/engine_metrics.go) admits ordered metric filters and rank stages, scalar and grouped spanset stages before metrics, and typed static hints. Top/bottom-k selection occurs independently at each timestamp. Fractional sampling is periodic, with scaling measured over admitted span or complete trace cohorts. Physical storage order can select different cohorts, so the three fractional live cases use independently enumerated complete output domains and retain each raw response; they do not claim exact cross-server equality. The other positive cases require complete metric label/point equality against an independent ledger. Scalar stages that consume real spans and reject every span-set preserve the initialized ungrouped count/rate series with a complete zero grid. Genuinely empty selectors, grouped metrics and sum/average aggregations do not initialize that series; independent controls cover these distinctions. The pinned [scalar-filter validator](https://github.com/grafana/tempo/blob/1900ed7bb5cad1a3edc285783d7d4ac4278337dc/pkg/traceql/ast.go) rejects arithmetic or comparisons between aggregate results even where its grammar admits them. Those cases remain explicit query rejections.

Prometheus histogram template values expose all 23 methods callable through Go templates. A separate 35-output Go ledger checks mutating method compositions, field and slice references, copied slice views, iterator headers, and replay isolation. Four exported methods have return signatures that Go templates reject; those remain error controls. The shared exponential bucket-bound helper uses the pinned source constants, with independent IEEE-754 endpoint controls for subnormal values and the last finite bucket. Separate pinned Go formatter goldens check typed fields, nil slices, all iterator cursor states and pointer flags. Pointer identity checks run within each process. Time-location controls check source field order, fixed-zone identities, lazy initialization and cache boundaries at load time. These goldens supplement the 55 helper cases above and retain the full query-language inventory denominator.
