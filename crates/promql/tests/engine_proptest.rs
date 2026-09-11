//! Property tests for the `PromQL` evaluation engine over data.
//!
//! The other generative tests in this repository stop at the front end.
//! `parser_proptest.rs` proves that the parser is total, that the AST survives
//! a render and a re-parse, and that rendering is idempotent.
//! `fuzz/fuzz_targets/promql_parse.rs` drives that same parser with arbitrary
//! bytes. Neither one puts a sample in a store. `conformance.rs` does evaluate
//! queries over data, but it replays a fixed corpus of hand-written cases from
//! upstream Prometheus, so it covers the shapes that corpus holds and no
//! others.
//!
//! These are the first properties in the repository that generate the data and
//! then evaluate a query over it. Each one states an invariant from the
//! Prometheus specification, computes the answer in the test from the samples
//! it generated, and compares that answer against the engine. The expected side
//! never calls the engine, so a bug in the engine cannot cancel itself out.

use std::{
    cmp::Ordering,
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use krabka_blockstore::{Labels, TenantId};
use krabka_promql::{EngineOpts, InMemoryMetricStore, PromqlEngine, QueryResult, SampleValue};
use proptest::{prelude::*, test_runner::TestCaseError};
use tokio::runtime::Runtime;

/// The single tenant every property writes to and reads from.
const TENANT: &str = "tenant-a";

/// The instant every property evaluates at. It sits far above zero, so a
/// property can place samples before it without a negative timestamp.
const EVAL_MS: i64 = 1_600_000;

/// A tokio runtime shared by every case in this file.
///
/// A `proptest!` body is a plain block, so it cannot carry `#[tokio::test]`.
/// Each case drives the engine through `block_on` on this runtime instead.
fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("build a tokio runtime for the property cases"))
}

/// Builds a label set from `(name, value)` pairs.
fn labels(pairs: &[(&str, &str)]) -> Labels {
    Labels::from_pairs(pairs.iter().map(|(name, value)| (*name, *value)))
}

/// Copies a label set into an owned map, so the expected side and the engine
/// side compare as the same type.
fn label_map(labels: &Labels) -> BTreeMap<String, String> {
    labels
        .iter()
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// One row of an instant vector, as both sides of a property state it.
type Row = (BTreeMap<String, String>, f64);

/// Orders rows, so two sets of rows compare in one assertion.
fn by_labels_then_value(left: &Row, right: &Row) -> Ordering {
    left.0.cmp(&right.0).then(left.1.total_cmp(&right.1))
}

/// Evaluates `query` at `EVAL_MS` and returns the instant vector as a sorted
/// vector of rows.
fn eval_sorted(
    engine: &PromqlEngine<InMemoryMetricStore>,
    query: &str,
) -> Result<Vec<Row>, TestCaseError> {
    let result = runtime()
        .block_on(engine.query_instant(
            &TenantId::new(TENANT).expect("the test tenant is a valid tenant id"),
            query,
            EVAL_MS,
        ))
        .map_err(|error| TestCaseError::fail(format!("{query}: {error}")))?;
    let QueryResult::InstantVector(samples) = result else {
        return Err(TestCaseError::fail(format!(
            "{query}: expected an instant vector, got {}",
            result.result_type()
        )));
    };
    let mut rows = Vec::with_capacity(samples.len());
    for sample in &samples {
        let SampleValue::Float(value) = &sample.value else {
            return Err(TestCaseError::fail(format!(
                "{query}: expected a float sample, got a native histogram"
            )));
        };
        rows.push((label_map(&sample.labels), *value));
    }
    rows.sort_by(by_labels_then_value);
    Ok(rows)
}

/// Sorts the rows a property computed for itself, so it can compare them
/// against the engine rows in one assertion.
fn sorted(mut rows: Vec<Row>) -> Vec<Row> {
    rows.sort_by(by_labels_then_value);
    rows
}

/// A series for the aggregation property: a `job` label index and a value.
///
/// The value is in half units. The property divides it by two, which keeps
/// every generated value exactly representable as an `f64`.
type AggregationSeries = (u8, i32);

/// Generates two or three `job` values, then the series that spread over them.
fn aggregation_input() -> impl Strategy<Value = Vec<AggregationSeries>> {
    (2u8..=3).prop_flat_map(|job_count| prop::collection::vec((0u8..job_count, -40i32..=40), 2..=6))
}

/// A scrape interval, held as milliseconds and as the same value in seconds.
///
/// The two forms are generated together, so the property never casts an
/// integer millisecond count to a float.
type ScrapeInterval = (i64, f64);

/// Generates the scrape interval, the per-scrape increment and the window
/// length for the `rate` property.
fn rate_input() -> impl Strategy<Value = (ScrapeInterval, f64, i64)> {
    (
        prop::sample::select(vec![
            (1_000_i64, 1.0_f64),
            (2_000, 2.0),
            (5_000, 5.0),
            (10_000, 10.0),
            (15_000, 15.0),
            (30_000, 30.0),
        ]),
        prop::sample::select(vec![0.5_f64, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0]),
        3_i64..=10,
    )
}

/// Generates distinct series values and a `k` that can exceed the series count.
fn topk_input() -> impl Strategy<Value = (Vec<f64>, usize)> {
    (2usize..=6).prop_flat_map(|series_count| {
        let values = (
            -40i32..=40,
            prop::collection::vec(1i32..=9, series_count - 1),
        )
            .prop_map(|(base, steps)| {
                // Each step is at least one half unit, so the values are
                // strictly increasing before the shuffle, and therefore
                // distinct. Half units are exactly representable as an `f64`.
                let mut current = base;
                let mut values = vec![f64::from(current) / 2.0];
                for step in steps {
                    current += step;
                    values.push(f64::from(current) / 2.0);
                }
                values
            })
            .prop_shuffle();
        (values, 1usize..=series_count + 2)
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// `sum`, `min`, `max` and `count` grouped by one label answer exactly the
    /// fold of the samples in each group.
    ///
    /// This is the Prometheus definition of an aggregation over an instant
    /// vector. `<op> by (job)` puts every series that shares a `job` value in
    /// one group, drops every other label including `__name__`, and folds the
    /// group with `op`. The sample the fold sees is the one an instant-vector
    /// selector picks: the most recent sample at or before the evaluation time,
    /// within the 5-minute lookback.
    ///
    /// Every generated series carries a sample at exactly the evaluation
    /// timestamp, so the lookback never has to choose. Each series also carries
    /// two earlier samples whose values are 100 away from the value at the
    /// evaluation timestamp. A selector that reaches past the sample at `t`
    /// changes the engine answer and nothing else.
    ///
    /// Values are half units, so every value, and every sum of at most six of
    /// them, is exactly representable as an `f64`. The property compares with
    /// `==`.
    ///
    /// The expected side is arithmetic over the generated samples. An engine
    /// that groups by the wrong key, that keeps `instance` in the output
    /// labels, that folds an empty group into a row, or that picks the wrong
    /// sample inside the lookback window moves the engine side away from it.
    #[test]
    fn an_instant_aggregation_matches_the_samples_it_aggregates(
        series in aggregation_input()
    ) {
        let mut store = InMemoryMetricStore::new();
        let mut points: Vec<(String, f64)> = Vec::with_capacity(series.len());
        for (index, (job_index, half_units)) in series.iter().enumerate() {
            let job = format!("job-{job_index}");
            let instance = format!("instance-{index}");
            let value = f64::from(*half_units) / 2.0;
            let series_labels = labels(&[
                ("__name__", "prop_gauge"),
                ("job", job.as_str()),
                ("instance", instance.as_str()),
            ]);
            // Decoys well outside the range of the generated values. They are
            // inside the 5-minute lookback, so only the rule "most recent
            // sample at or before `t`" keeps them out of the answer.
            store.push_float(TENANT, series_labels.clone(), EVAL_MS - 30_000, value + 100.0);
            store.push_float(TENANT, series_labels.clone(), EVAL_MS - 15_000, value - 100.0);
            store.push_float(TENANT, series_labels, EVAL_MS, value);
            points.push((job, value));
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let mut groups: BTreeMap<String, Vec<f64>> = BTreeMap::new();
        for (job, value) in &points {
            groups.entry(job.clone()).or_default().push(*value);
        }

        for op in ["sum", "min", "max", "count"] {
            let want = sorted(
                groups
                    .iter()
                    .map(|(job, values)| {
                        let folded = match op {
                            "sum" => values.iter().sum::<f64>(),
                            "min" => values.iter().copied().fold(f64::INFINITY, f64::min),
                            "max" => values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
                            _ => values.iter().fold(0.0_f64, |count, _| count + 1.0),
                        };
                        let group_labels =
                            BTreeMap::from([("job".to_string(), job.clone())]);
                        (group_labels, folded)
                    })
                    .collect(),
            );
            let query = format!("{op} by (job) (prop_gauge)");
            let got = eval_sorted(&engine, &query)?;
            prop_assert_eq!(got, want, "{}", query);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// `rate` over a counter that gains a fixed amount every scrape answers
    /// that amount per second.
    ///
    /// The derivation comes from the Prometheus definition of `rate`, not from
    /// the engine. Write `s` for the scrape interval in milliseconds, `c` for
    /// the per-scrape increment, `k` for the number of scrapes in the window,
    /// and `d = k * s` for the window length. The evaluation time `t` sits on a
    /// sample, and the series runs from well before `t - d` up to `t`.
    ///
    /// A range selector `[d]` at `t` takes the samples in `(t - d, t]`. On a
    /// series of spacing `s` aligned to `t`, that is `k` samples, spanning
    /// `d - s` and rising by `(k - 1) * c`. Prometheus then extrapolates that
    /// span out to the window edges. The gap to each edge is `s` or less, and
    /// the average sample interval is `s`, so both gaps are under the
    /// `1.1 * s` extrapolation threshold, and both extrapolate in full. The
    /// counter also stays above its own zero point, so the zero-crossing clamp
    /// never applies. The extrapolation is therefore
    /// `factor = d / (d - s)`, and the result in samples per second is
    ///
    /// ```text
    /// (k - 1) * c * factor / (d / 1000)
    ///   = (k - 1) * c * (d / (d - s)) * 1000 / d
    ///   = (k - 1) * c * 1000 / ((k - 1) * s)
    ///   = c / (s / 1000)
    /// ```
    ///
    /// The window boundary convention does not change this. If the sample at
    /// exactly `t - d` counted as well, the span would be `d`, the rise
    /// `k * c`, and the factor 1, which gives the same answer.
    ///
    /// The expected side is that closed form. An engine that drops the
    /// extrapolation factor, that divides by the wrong number of seconds, or
    /// that mis-clamps the extrapolation at an edge moves the engine side away
    /// from it.
    #[test]
    fn rate_over_a_constant_increment_counter_is_that_increment_per_second(
        ((scrape_ms, scrape_secs), increment, scrapes) in rate_input()
    ) {
        let window_ms = scrapes * scrape_ms;
        // Twice as many scrapes as the window holds, so the window sits well
        // inside the series and the left edge is never the start of the data.
        let first_ms = EVAL_MS - 2 * scrapes * scrape_ms;
        let mut store = InMemoryMetricStore::new();
        let mut value = 0.0_f64;
        let mut ts_ms = first_ms;
        while ts_ms <= EVAL_MS {
            store.push_float(TENANT, labels(&[("__name__", "prop_counter")]), ts_ms, value);
            value += increment;
            ts_ms += scrape_ms;
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let want = increment / scrape_secs;
        let query = format!("rate(prop_counter[{window_ms}ms])");
        let got = eval_sorted(&engine, &query)?;
        prop_assert_eq!(got.len(), 1, "{}", query);
        // `rate` drops `__name__`, and the series carries no other label.
        prop_assert!(got[0].0.is_empty(), "{}: {:?}", query, got[0].0);
        prop_assert!(
            (got[0].1 - want).abs() <= 1e-9 * want.abs(),
            "{}: got {}, want {}",
            query,
            got[0].1,
            want
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// `topk(k, v)` answers the `k` largest samples of `v`, and `bottomk(k, v)`
    /// the `k` smallest. Both keep the labels of the series they select.
    ///
    /// This is the Prometheus definition of the two selection aggregators. They
    /// are the aggregators that do not drop `__name__`, because they return the
    /// input samples rather than a fold of them. When `k` is at least the
    /// number of series, both return every series.
    ///
    /// The generated values are strictly increasing before a shuffle, so they
    /// are distinct. Prometheus does not specify how `topk` breaks a tie, so a
    /// repeated value would leave the answer ambiguous and the property could
    /// not state it.
    ///
    /// The expected side sorts the generated values in the test and cuts the
    /// vector at `min(k, series)`. An engine that returns the wrong end of the
    /// order, that returns the wrong count, that drops `__name__`, or that
    /// mishandles a `k` above the series count moves the engine side away from
    /// it.
    #[test]
    fn topk_returns_exactly_the_k_largest_samples((values, k) in topk_input()) {
        let mut store = InMemoryMetricStore::new();
        let mut rows: Vec<Row> = Vec::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            let instance = format!("instance-{index}");
            let series_labels = labels(&[
                ("__name__", "prop_topk"),
                ("instance", instance.as_str()),
            ]);
            // An earlier decoy, so the answer depends on the sample at `t`.
            store.push_float(TENANT, series_labels.clone(), EVAL_MS - 30_000, value + 100.0);
            store.push_float(TENANT, series_labels.clone(), EVAL_MS, *value);
            rows.push((label_map(&series_labels), *value));
        }
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

        let taken = k.min(values.len());
        let mut ascending = rows.clone();
        ascending.sort_by(by_labels_then_value);
        ascending.sort_by(|left, right| left.1.total_cmp(&right.1));

        let want_top = sorted(ascending.iter().rev().take(taken).cloned().collect());
        let top_query = format!("topk({k}, prop_topk)");
        let got_top = eval_sorted(&engine, &top_query)?;
        prop_assert_eq!(got_top, want_top, "{}", top_query);

        let want_bottom = sorted(ascending.iter().take(taken).cloned().collect());
        let bottom_query = format!("bottomk({k}, prop_topk)");
        let got_bottom = eval_sorted(&engine, &bottom_query)?;
        prop_assert_eq!(got_bottom, want_bottom, "{}", bottom_query);
    }
}
