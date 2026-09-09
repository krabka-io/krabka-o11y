# Vendored Prometheus PromQL conformance tests

These `.test` files are a **curated subset** of Prometheus's PromQL test corpus,
copied from:

- Upstream: https://github.com/prometheus/prometheus
- Path: `promql/promqltest/testdata/*.test`
- Pinned tags:
  - `functions.test`, `ranges.test`, `staleness.test`:
    `v3.5.0` (commit `8be3a9560fbdd18a94dedec4b747c35178177202`)
  - `aggregators.test`, `at_modifier.test`, `collision.test`,
    `duration_expression.test`, `extended_vectors.test`, `info.test`,
    `limit.test`, `literals.test`, `name_label_dropping.test`,
    `native_histograms.test`, `operators.test`, `range_queries.test`,
    `selectors.test`, `subquery.test`, `trig_functions.test`,
    `type_and_unit.test`: `v3.8.1` (commit
    `ed753444ffec98097399d0cfa9073c70a840b812`)

Prometheus is licensed under the Apache License 2.0. The full license text is in
the upstream `LICENSE` file. These files retain their original copyright; they are
used here with final whitespace normalized.

The conformance gate covers every case checked into this directory; it does not
claim coverage of every upstream case. Large upstream files are intentionally
vendored only in part. `name_label_dropping.test` omits the v3.8.1 cases for
delayed `__name__` removal through range functions, label rewrites, and nested
aggregations because that query-engine feature is not implemented here.
`aggregators.test` vendors the `expect ordered` topk/bottomk cases but omits the
one that reads its `k` from a query, `topk(scalar(foo), http_requests)`, because
the engine accepts only a number literal there. `functions.test` omits the two
ordered cases of the upstream sort/sort_desc section, `sort(http_requests)` and
`sort_desc(http_requests)`. The two cases share one `load` block, and that block
holds a native-histogram series. The planner declines a `sort` or a `sort_desc`
whose selector matches a histogram series, and the engine then fails the query.
Prometheus instead drops the histogram sample and sorts the float samples that
remain.
