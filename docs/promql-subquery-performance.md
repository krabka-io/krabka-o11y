# PromQL subquery grid reuse

Issue [#379](https://github.com/krabka-io/krabka-o11y/issues/379) concerns repeated inner evaluation across overlapping subquery windows. The range driver keeps one bounded union grid per subquery. Each outer step selects its own aligned window from that grid.

The memo belongs to one range driver. Its key includes the expression, range, resolution, offset, and `@` modifier. The driver scope fixes the tenant, engine options, query bounds, and experimental range context. Union results share the existing two-million-point budget with leaf results. A union that exceeds the resolution cap or memo budget uses the original window path. A speculative union error also uses that path, so wider scans cannot reject otherwise valid windows.

Finite union materialization admits float results with a stable metric-name policy. Histogram results keep the original path. Plain annotations can use the memo when the first window emits the union's complete annotation set in the same order. That check needs one extra window evaluation. Structured histogram repairs keep the original path because their counts accumulate. Queries that request sample statistics bypass the union memo.

## Measurement

The query was `max_over_time(rate(http_requests_total[5m])[Wm:15s])`. The outer step and inner resolution were both 15 seconds. The first outer timestamp was 3,600 seconds. The selected counter had a `__type__="counter"` label and samples every 15 seconds. The fixture is in [`subquery_grid.rs`](../crates/promql/src/engine/tests/subquery_grid.rs).

These are single runs on Linux x86-64 with five available CPUs and 17 GiB of memory. The binary used Rust 1.99.1 and the Cargo test profile without optimization. Each case ran in a separate process. CPU is user plus system time. Peak RSS covers the whole test process, including the fixture and DataFusion. Python `os.wait4` supplied the process statistics. These measurements do not establish a production latency result or a release benchmark baseline.

The uncached cases disable only the subquery union memo. Existing scan and leaf caches remain active.

| Outer steps | Inner window | Scans before | Scans after | CPU before (ms) | CPU after (ms) | Peak RSS before (KiB) | Peak RSS after (KiB) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 30 | 5 min | 30 | 1 | 460 | 57 | 113016 | 112808 |
| 30 | 60 min | 30 | 1 | 642 | 73 | 114188 | 113756 |
| 120 | 5 min | 120 | 1 | 1662 | 51 | 112888 | 113340 |
| 120 | 60 min | 120 | 1 | 3488 | 105 | 114196 | 114176 |

## Reproduce

Build the test binary with `cargo test -p krabka-promql --lib --locked --no-run`. Set `PROMQL_TEST_BIN` to the executable path that Cargo prints. Run each measurement in a separate process after compilation:

```bash
/usr/bin/time -v env SUBQUERY_OUTER_STEPS=120 SUBQUERY_WINDOW_MINUTES=60 \
  "$PROMQL_TEST_BIN" engine::tests::subquery_grid::subquery_grid_profile \
  --exact --ignored --nocapture

/usr/bin/time -v env SUBQUERY_OUTER_STEPS=120 SUBQUERY_WINDOW_MINUTES=60 \
  SUBQUERY_UNCACHED=1 \
  "$PROMQL_TEST_BIN" engine::tests::subquery_grid::subquery_grid_profile \
  --exact --ignored --nocapture
```

Vary `SUBQUERY_OUTER_STEPS` and `SUBQUERY_WINDOW_MINUTES` independently. The test prints the store scan count and elapsed query time. The process monitor supplies CPU and peak RSS. On Linux without GNU time, use `os.wait4` and read `ru_utime`, `ru_stime`, and `ru_maxrss`.

## Correctness checks

The regression tests compare cached and uncached windows and compare each outer step with the interpreter. Cases cover unaligned outer steps, explicit and default resolution, positive and negative offsets, `@ start()` and `@ end()`, staleness, absent series, scalar parameters, nested subqueries, annotations, histograms, sample statistics, query isolation, tenant isolation, sample limits, series limits, and resolution limits. A bounded-materialization test checks the memo's point budget.

Run them with `cargo test -p krabka-promql --lib subquery_grid --locked`. Run the existing subquery parity cases with `cargo test -p krabka-promql --lib subquery_planner_path_matches_interpreter --locked`.
