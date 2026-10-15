use super::*;

mod a_comparison_keeps_the_metric_name_that_arithmetic_drops {
use super::*;

#[derive(Clone)]
struct ComparisonInput {
    metric: &'static str,
    job: &'static str,
    extra_label: Option<InputLabel>,
    value: f64,
}

#[derive(Clone)]
struct InputLabel {
    name: &'static str,
    value: &'static str,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedLabel {
    name: String,
    value: String,
}

impl ExpectedLabel {
    fn new(name: &str, value: &str) -> Self {
        Self {
            name: name.to_owned(),
            value: value.to_owned(),
        }
    }
}

fn expected_job_labels() -> Vec<Vec<ExpectedLabel>> {
    ["x", "y"]
        .into_iter()
        .map(|job| vec![ExpectedLabel::new("job", job)])
        .collect()
}

fn expected_instance_job_labels(keep_metric_name: bool) -> Vec<Vec<ExpectedLabel>> {
    ["1", "9"]
        .into_iter()
        .zip(["x", "y"])
        .map(|(instance, job)| {
            let mut labels = Vec::new();
            if keep_metric_name {
                labels.push(ExpectedLabel::new("__name__", "m"));
            }
            labels.push(ExpectedLabel::new("inst", instance));
            labels.push(ExpectedLabel::new("job", job));
            labels
        })
        .collect()
}

struct QueryExpectation {
    query: &'static str,
    labels: Vec<Vec<ExpectedLabel>>,
}

/// One-to-one `on` projects comparison labels to the matching set. Grouped
/// comparisons preserve operand metadata while arithmetic drops metric names.
/// Each query pair exercises matched and filled paths against explicit labels.
#[tokio::test]
pub(crate) async fn a_comparison_keeps_the_metric_name_that_arithmetic_drops() {
    let mut store = InMemoryMetricStore::new();
    for input in [
        ComparisonInput {
            metric: "a",
            job: "x",
            extra_label: Some(InputLabel {
                name: "extra",
                value: "1",
            }),
            value: 5.0,
        },
        ComparisonInput {
            metric: "a",
            job: "y",
            extra_label: Some(InputLabel {
                name: "extra",
                value: "2",
            }),
            value: 5.0,
        },
        ComparisonInput {
            metric: "b",
            job: "x",
            extra_label: None,
            value: 3.0,
        },
        ComparisonInput {
            metric: "m",
            job: "x",
            extra_label: Some(InputLabel {
                name: "inst",
                value: "1",
            }),
            value: 7.0,
        },
        ComparisonInput {
            metric: "m",
            job: "y",
            extra_label: Some(InputLabel {
                name: "inst",
                value: "9",
            }),
            value: 7.0,
        },
    ] {
        let mut pairs = vec![
            krabka_metrics::MetricLabelPair::new("__name__", input.metric),
            krabka_metrics::MetricLabelPair::new("job", input.job),
        ];
        if let Some(extra) = input.extra_label {
            pairs.push(krabka_metrics::MetricLabelPair::new(
                extra.name,
                extra.value,
            ));
        }
        store.push_float(
            "tenant-a",
            crate::PromqlLabels::from_pairs(pairs),
            10_000,
            input.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        // One-to-one: `x` matches, `y` takes the right-fill path.
        QueryExpectation {
            query: "a > on (job) fill_right(0) b",
            labels: expected_job_labels(),
        },
        QueryExpectation {
            query: "a + on (job) fill_right(0) b",
            labels: expected_job_labels(),
        },
        // The mirror of the pair above: here the *left* side is filled, so the
        // synthetic left operand has the matching labels and no metric name.
        // Only a left-fill query reaches that branch.
        QueryExpectation {
            query: "b < on (job) fill_left(0) a",
            labels: expected_job_labels(),
        },
        QueryExpectation {
            query: "b + on (job) fill_left(0) a",
            labels: expected_job_labels(),
        },
        // `bool` puts a comparison back on the arithmetic rule.
        QueryExpectation {
            query: "a > bool on (job) b",
            labels: vec![vec![ExpectedLabel::new("job", "x")]],
        },
        // Many-to-one: `x` matches, `y` takes the right-fill path.
        QueryExpectation {
            query: "m > on (job) group_left fill_right(0) b",
            labels: expected_instance_job_labels(true),
        },
        QueryExpectation {
            query: "m + on (job) group_left fill_right(0) b",
            labels: expected_instance_job_labels(false),
        },
        // One-to-many: `x` matches, `y` takes the left-fill path.
        QueryExpectation {
            query: "b < on (job) group_right fill_right(0) m",
            labels: expected_instance_job_labels(true),
        },
        QueryExpectation {
            query: "b + on (job) group_right fill_right(0) m",
            labels: expected_instance_job_labels(false),
        },
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), case.query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", case.query));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {}", case.query);
        };
        let mut got = samples
            .iter()
            .map(|sample| {
                sample
                    .labels
                    .iter()
                    .map(|pair| ExpectedLabel {
                        name: pair.name.clone(),
                        value: pair.value.to_string(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        got.sort();
        let want = case.labels.iter().cloned().collect::<Vec<_>>();
        assert2::assert!(got == want, "{}", case.query);
    }
}

}

mod a_fill_modifier_is_refused_on_a_set_operator {
use super::*;

/// Fill modifiers are meaningless for set operators and are refused. Each
/// form below sets only one side, so a guard that joined the two checks with
/// `&&` instead of `||` would accept the single-sided ones -- `fill(0)` sets
/// both and is refused either way.
#[tokio::test]
pub(crate) async fn a_fill_modifier_is_refused_on_a_set_operator() {
    let mut store = InMemoryMetricStore::new();
    for name in ["a", "b"] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", name),
                crate::test_support::TestLabelPair::new("job", "x"),
            ]),
            10_000,
            1.0,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for query in [
        "a and on (job) fill_left(0) b",
        "a or on (job) fill_right(0) b",
        "a unless on (job) fill(0) b",
    ] {
        let error = engine
            .query_instant(&tenant_id("tenant-a"), query, 10_000)
            .await
            .expect_err("a fill modifier on a set operator is a planning error");
        assert2::assert!(
            matches!(
                &error,
                PromqlError::Plan(message)
                    if message.contains("fill modifiers are invalid for set operators")
            ),
            "{query}: {error}"
        );
    }
}

}

mod binary_planner_path_matches_interpreter {
use super::*;

#[derive(bon::Builder)]
struct VectorSampleCase {
    metric_name: &'static str,
    job: &'static str,
    code: &'static str,
    instance: &'static str,
    value: f64,
}

struct ScalarResultCase {
    query: &'static str,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn binary_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store with overlapping label dimensions for vector
    // matching. `left`/`right` share `{job}` for one-to-one and group_x
    // matching; `code` differentiates the many side. A NaN row and a series
    // present only on one side exercise NaN preservation and no-match drops.
    let mut store = InMemoryMetricStore::new();
    for case in [
        // `left`: one per job (the "one" side for group_left).
        VectorSampleCase::builder()
            .metric_name("left")
            .job("api")
            .code("")
            .instance("")
            .value(10.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("left")
            .job("db")
            .code("")
            .instance("")
            .value(20.0)
            .build(),
        // `right`: many per job (`code` dimension), the "many" side.
        VectorSampleCase::builder()
            .metric_name("right")
            .job("api")
            .code("200")
            .instance("")
            .value(1.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("right")
            .job("api")
            .code("500")
            .instance("")
            .value(2.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("right")
            .job("db")
            .code("200")
            .instance("")
            .value(4.0)
            .build(),
        // A `right` series whose job has no `left` match (no-match drop).
        VectorSampleCase::builder()
            .metric_name("right")
            .job("web")
            .code("200")
            .instance("")
            .value(8.0)
            .build(),
        // `m1`/`m2` for one-to-one on/ignoring matching.
        VectorSampleCase::builder()
            .metric_name("m1")
            .job("api")
            .code("")
            .instance("0")
            .value(3.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("m1")
            .job("api")
            .code("")
            .instance("1")
            .value(5.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("m2")
            .job("api")
            .code("")
            .instance("0")
            .value(7.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("m2")
            .job("api")
            .code("")
            .instance("1")
            .value(11.0)
            .build(),
        // A genuine-NaN row that must survive vector∘scalar arithmetic.
        VectorSampleCase::builder()
            .metric_name("nanm")
            .job("api")
            .code("")
            .instance("0")
            .value(f64::NAN)
            .build(),
        VectorSampleCase::builder()
            .metric_name("nanm")
            .job("api")
            .code("")
            .instance("1")
            .value(13.0)
            .build(),
    ] {
        let mut pairs = vec![
            krabka_metrics::MetricLabelPair::new("__name__", case.metric_name),
            krabka_metrics::MetricLabelPair::new("job", case.job),
        ];
        if !case.code.is_empty() {
            pairs.push(krabka_metrics::MetricLabelPair::new("code", case.code));
        }
        if !case.instance.is_empty() {
            pairs.push(krabka_metrics::MetricLabelPair::new(
                "instance",
                case.instance,
            ));
        }
        store.push_float(
            "t",
            crate::PromqlLabels::from_pairs(pairs),
            60_000,
            case.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 60_000_i64;

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware).
    let queries = [
        // vector ∘ scalar (arithmetic, drops __name__).
        "m1 + 100",
        "m1 * 2",
        // scalar ∘ vector.
        "100 - m1",
        "2 ^ m1",
        // vector ∘ scalar comparison without bool (filters, keeps labelset).
        "m1 > 4",
        // vector ∘ scalar comparison with bool (keeps all, drops __name__).
        "m1 > bool 4",
        // genuine NaN must survive vector∘scalar arithmetic.
        "nanm + 1",
        // vector ∘ vector one-to-one, default matching (drops __name__).
        "m1 + m2",
        "m2 - m1",
        "m1 / m2",
        "m1 % m2",
        "m1 ^ m2",
        "m1 atan2 m2",
        // one-to-one with on / ignoring.
        "m1 + on(job, instance) m2",
        "m1 + ignoring(__name__) m2",
        // one-to-one comparison without bool (keeps LHS labelset incl. name).
        "m1 > m2",
        "m2 >= m1",
        // one-to-one comparison with bool (drops __name__).
        "m1 == bool m2",
        "m1 != bool m2",
        // group_left (many-to-one): the `right` many side copies a label
        // from the `left` one side.
        "right * on(job) group_left left",
        "right + on(job) group_left() left",
        // group_right (one-to-many): the `left` one side, many `right`.
        "left * on(job) group_right right",
        // set ops: and / or / unless, with and without on/ignoring.
        "m1 and m2",
        "m1 or m2",
        "m1 unless m2",
        "right and on(job) left",
        "right unless on(job) left",
        "left or on(job) right",
        // a no-match set op (web has no left): or keeps it, and/unless drop.
        "right and on(job) left",
    ];

    for query in queries {
        let SortedParity {
            via_operators: operators,
            via_interpreter: interpreter,
        } = sorted_planned_and_interpreted(&engine, query, time_ms).await;
        assert2::assert!(nan_equal_samples(&interpreter, &operators));
    }

    // Pin specific behaviors the parity above relies on.
    // 1. `__name__` is dropped for arithmetic.
    let arith = engine
        .query_instant(&tenant_id("t"), "m1 + m2", time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(arith) = arith else {
        panic!("expected vector");
    };
    assert2::assert!(arith.iter().all(|s| s.labels.get("__name__").is_none()));
    // 2. A comparison without `bool` keeps the LHS labelset (incl. __name__).
    let cmp = engine
        .query_instant(&tenant_id("t"), "m1 > m2", time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(cmp) = cmp else {
        panic!("expected vector");
    };
    assert2::assert!(cmp.iter().all(|s| s.labels.get("__name__") == Some("m1")));
    // 3. A no-match set op: `right and on(job) left` drops `web` (no left).
    let setop = engine
        .query_instant(&tenant_id("t"), "right and on(job) left", time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(setop) = setop else {
        panic!("expected vector");
    };
    assert2::assert!(setop.iter().all(|s| s.labels.get("job") != Some("web")));

    // Scalar ∘ scalar now folds through the planner into a scalar planned
    // result; it must route AND match the interpreter's scalar value+ts.
    for case in [
        ScalarResultCase {
            query: "1 + 2",
            expected: 3.0_f64,
        },
        ScalarResultCase {
            query: "3 * 4 - 1",
            expected: 11.0_f64,
        },
        ScalarResultCase {
            query: "2 > bool 1",
            expected: 1.0_f64,
        },
    ] {
        let expr =
            parse_promql_with_duration_context(case.query, DurationExprContext::instant(time_ms))
                .unwrap();
        let planned = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap()
            .unwrap_or_else(|| {
                panic!(
                    "scalar∘scalar `{}` must route through the planner",
                    case.query
                )
            });
        let via_operators = engine
            .assemble_planned_instant(planned, time_ms)
            .await
            .unwrap();
        let QueryResult::Scalar { ts_ms, value } = via_operators else {
            panic!("expected scalar for `{}`", case.query);
        };
        assert2::assert!(ts_ms == time_ms);
        assert2::assert!(value.to_bits() == case.expected.to_bits());
    }
}

}

mod comparison_bool_returns_one_or_zero {
use super::*;

#[tokio::test]
pub(crate) async fn comparison_bool_returns_one_or_zero() {
    let engine = comparison_test_engine();
    let samples = instant_vector(&engine, "a > bool 0", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}

}

mod comparison_without_bool_filters_false_samples {
use super::*;

#[tokio::test]
pub(crate) async fn comparison_without_bool_filters_false_samples() {
    let engine = comparison_test_engine();
    let samples = instant_vector(&engine, "a > 100", 10_000).await;
    assert2::assert!(samples.is_empty());
}

}

mod group_left_does_not_copy_a_metadata_label_from_the_one_side {
use super::*;

/// `group_left (...)` copies the named labels off the one side. A metadata
/// label named there must still be skipped, or every result row inherits the
/// *other* operand's metric name.
#[tokio::test]
pub(crate) async fn group_left_does_not_copy_a_metadata_label_from_the_one_side() {
    let mut store = InMemoryMetricStore::new();
    for case in [
        InstanceValueCase {
            instance: "1",
            value: 3.0,
        },
        InstanceValueCase {
            instance: "2",
            value: 4.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "a"),
                crate::test_support::TestLabelPair::new("job", "x"),
                crate::test_support::TestLabelPair::new("instance", case.instance),
            ]),
            10_000,
            case.value,
        );
    }
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "b"),
            crate::test_support::TestLabelPair::new("job", "x"),
        ]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "a + on (job) group_left(__name__) b", 10_000).await;
    assert2::assert!(samples.len() == 2);
    for sample in &samples {
        assert2::assert!(sample.labels.get("__name__") == None);
    }
}

}

mod matching_on_a_metadata_label_keeps_it_out_of_the_result {
use super::*;

/// `on (...)` builds the result labels from the clause itself, so naming a
/// metadata label there would otherwise carry `__name__` into the output of an
/// arithmetic expression -- which never has one.
#[tokio::test]
pub(crate) async fn matching_on_a_metadata_label_keeps_it_out_of_the_result() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "x"),
            crate::test_support::TestLabelPair::new("job", "a"),
        ]),
        10_000,
        7.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "x - on (__name__) x", 10_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.iter().count() == 0);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 0.0));
}

}

mod one_to_one_fill_subtracts_in_the_declared_operand_order {
use super::*;

/// Fills are not commutative, but every fill test above uses `+`, where
/// `0 + present` and `present + 0` agree -- so the operand order on both fill
/// paths was free to be backwards. `-` tells them apart: a series present only
/// on the right must yield `0 - value`, not `value - 0`.
#[tokio::test]
pub(crate) async fn one_to_one_fill_subtracts_in_the_declared_operand_order() {
    let mut store = InMemoryMetricStore::new();
    push_metric_job_samples(
        &mut store,
        [
            ("a", "shared", 3.0),
            ("a", "left_only", 4.0),
            ("b", "shared", 1.0),
            ("b", "right_only", 5.0),
        ],
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "a - on (job) fill(0) b", 10_000).await;
    let mut values = BTreeMap::new();
    for sample in &samples {
        values.insert(
            sample.labels.get("job").expect("job label"),
            float_value(&sample.value),
        );
    }
    assert2::assert!(values.len() == 3);
    assert2::assert!(approx_eq(values["shared"], 2.0));
    assert2::assert!(approx_eq(values["left_only"], 4.0));
    assert2::assert!(approx_eq(values["right_only"], -5.0));
}

}

mod scalar_binary_arithmetic_returns_scalar {
use super::*;

#[tokio::test]
pub(crate) async fn scalar_binary_arithmetic_returns_scalar() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "2 * 3", 10_000)
        .await
        .unwrap();
    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: 6.0
            }
    );
}

}

mod scalar_binary_atan2_returns_angle_radians {
use super::*;

#[tokio::test]
pub(crate) async fn scalar_binary_atan2_returns_angle_radians() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "1 atan2 1", 10_000)
        .await
        .unwrap();
    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 10_000,
                value: std::f64::consts::FRAC_PI_4,
            }
    );
}

}


mod shared_series_labels_follow_snapshots_and_limits {
use super::*;
use krabka_blockstore::MatchOp;

use super::*;
use crate::{MergedMetricStore, MetricStore, PromqlMatcher as LabelMatcher, WalHead};

fn labels(name: &str, job: &str) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    labels.insert("job", job);
    labels
}

#[tokio::test]
async fn shared_series_labels_follow_snapshots_and_limits() {
    let api = labels("up", "api");
    let latency = labels("latency", "api");
    let mut cold = InMemoryMetricStore::new();
    cold.push_float("t", api.clone(), 1_000, 1.0);
    let cold_labels = Arc::clone(&cold.floats["t"].iter().next().unwrap().labels);
    let mut hot = InMemoryMetricStore::with_retention(secs(1));
    hot.push_float("t", api.clone(), 2_000, 2.0);
    hot.push_float("t", labels("up", "worker"), 999, 3.0);
    hot.push_histogram("t", latency.clone(), 2_000, native_histogram(2.0, 3.0));
    hot.push_float("other", labels("outside", "api"), 2_000, 4.0);
    let hot_labels = Arc::clone(&hot.hists["t"].iter().next().unwrap().labels);
    let head = WalHead::from_store(hot);
    let store = Arc::new(MergedMetricStore::new(cold, head.clone()));
    let engine = PromqlEngine::new(Arc::clone(&store), EngineOpts::default());
    let matchers = vec![LabelMatcher::new("job", MatchOp::Eq, "api")];
    let resolved = engine
        .labels_by_fingerprint_sets("t", std::slice::from_ref(&matchers), 1_000, 2_000)
        .await
        .unwrap();
    let mut expected: BTreeMap<_, crate::PromqlLabels> = BTreeMap::new();
    expected.insert(api.fingerprint(), api.clone().into());
    expected.insert(latency.fingerprint(), latency.clone().into());
    let hot_series = head
        .series_shared("t", &matchers, 1_000, 2_000)
        .await
        .unwrap();
    check!(
        hot_series.iter().map(Arc::as_ref).collect::<Vec<_>>()
            == expected.values().collect::<Vec<_>>()
    );
    check!(
        resolved
            .iter()
            .map(|(fp, labels)| (*fp, labels.as_ref().clone()))
            .collect::<BTreeMap<_, _>>()
            == expected
    );
    check!(Arc::ptr_eq(&resolved[&api.fingerprint()], &cold_labels));
    check!(Arc::ptr_eq(&resolved[&latency.fingerprint()], &hot_labels));
    let owned = store.series("t", &matchers, 1_000, 2_000).await.unwrap();
    check!(
        owned
            .into_iter()
            .map(|labels| (labels.fingerprint(), labels))
            .collect::<BTreeMap<_, _>>()
            == expected
    );
    let limited = PromqlEngine::new(
        Arc::clone(&store),
        EngineOpts {
            max_fetched_series: 1,
            ..EngineOpts::default()
        },
    );
    check!(
        limited
            .labels_by_fingerprint_sets("t", std::slice::from_ref(&matchers), 1_000, 2_000)
            .await
            .is_err()
    );
    let pruned = head.prune(4_000);
    check!((pruned.samples_dropped, pruned.series_dropped) == (4, 4));
    head.delete_tenant("t");
    let current = engine
        .labels_by_fingerprint_sets("t", &[matchers], 1_000, 2_000)
        .await
        .unwrap();
    check!(current.len() == 1);
    check!(resolved[&latency.fingerprint()].as_ref() == &latency);
    check!(Arc::ptr_eq(&resolved[&latency.fingerprint()], &hot_labels));
}
}
