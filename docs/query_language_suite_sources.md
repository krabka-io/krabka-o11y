# Query-language suite implementation sources

Source inspection on 2026-10-05. Commands below require running reference and Krabka endpoints with identical seeded data; they are setup instructions, not passing-test evidence. Downloaded source excerpts, original licences, a SHA-256 manifest, the complete Loki YAML catalogue, and independent expected-value examples are under `/home/matt/.codex/tmp/query-conformance-research/`.

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
