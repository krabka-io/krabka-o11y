use std::cell::RefCell;

use super::{
    super::annotations::{ANNOTATION_SOURCE, ANNOTATIONS},
    *,
};
use crate::{Annotations, DurationExprContext, parse_promql_with_duration_context};

struct CounterMetricFixture {
    metric: &'static str,
    env: &'static str,
    scale: f64,
    metric_type: Option<&'static str>,
}

struct CounterQueryCase {
    query: &'static str,
    intercept: f64,
    slope: f64,
    annotation_column: Option<usize>,
}

struct NameRetentionCase {
    query: &'static str,
    metric_name: Option<&'static str>,
}

#[tokio::test]
async fn counter_warning_composed_paths_preserve_origin_and_position() {
    let mut store = InMemoryMetricStore::new();
    for seconds in (0..=140).step_by(10) {
        for fixture in [
            CounterMetricFixture {
                metric: "metric_total",
                env: "1",
                scale: 1.0,
                metric_type: None,
            },
            CounterMetricFixture {
                metric: "metric_total",
                env: "2",
                scale: 2.0,
                metric_type: None,
            },
            CounterMetricFixture {
                metric: "typed_total",
                env: "typed",
                scale: 1.0,
                metric_type: Some("counter"),
            },
        ] {
            let mut labels = labels(&[
                crate::test_support::TestLabelPair::new("__name__", fixture.metric),
                crate::test_support::TestLabelPair::new("env", fixture.env),
            ]);
            if let Some(metric_type) = fixture.metric_type {
                labels.insert("__type__", metric_type);
            }
            store.push_float(
                "t",
                labels,
                seconds * 1_000,
                f64::from(i32::try_from(seconds).unwrap()) * fixture.scale,
            );
        }
    }
    let store = Arc::new(store);
    // Independent input ledger: metric_total/env1 rises by 10 per ten seconds,
    // env2 by 20. A three-point rolling sum rises by 30 per ten seconds.
    let cases = [
        CounterQueryCase {
            query: "rate({env=\"1\"}[1m])",
            intercept: 1.0,
            slope: 0.0,
            annotation_column: Some(6),
        },
        CounterQueryCase {
            query: "increase({env=\"1\"}[1m])",
            intercept: 60.0,
            slope: 0.0,
            annotation_column: Some(10),
        },
        CounterQueryCase {
            query: "sum(rate({env=\"1\"}[1m])) by (env)",
            intercept: 1.0,
            slope: 0.0,
            annotation_column: Some(10),
        },
        CounterQueryCase {
            query: "label_join(rate({env=\"1\"}[1m]),\"name\",\"_\",\"__name__\")",
            intercept: 1.0,
            slope: 0.0,
            annotation_column: Some(17),
        },
        CounterQueryCase {
            query: "label_replace(rate({env=\"1\"}[1m]),\"name\",\"$1\",\"__name__\",\"(.+)\")",
            intercept: 1.0,
            slope: 0.0,
            annotation_column: Some(20),
        },
        CounterQueryCase {
            query: "sum by (__name__) (rate(metric_total{env=\"1\"}[1m]) or metric_total{env=\"2\"})",
            intercept: 1.0,
            slope: 2.0,
            annotation_column: Some(25),
        },
        CounterQueryCase {
            query: "rate(sum_over_time(metric_total{env=\"1\"}[30s:10s])[50s:10s])",
            intercept: 3.0,
            slope: 0.0,
            annotation_column: Some(1),
        },
        CounterQueryCase {
            query: "increase(sum_over_time(metric_total{env=\"1\"}[30s:10s])[50s:10s])",
            intercept: 150.0,
            slope: 0.0,
            annotation_column: Some(1),
        },
        CounterQueryCase {
            query: "rate(last_over_time(metric_total{env=\"1\"}[30s:10s])[50s:10s])",
            intercept: 1.0,
            slope: 0.0,
            annotation_column: Some(1),
        },
        CounterQueryCase {
            query: "rate(typed_total[1m])",
            intercept: 1.0,
            slope: 0.0,
            annotation_column: None,
        },
        // Vector/vector arithmetic changes metric schema eagerly; scalar
        // arithmetic retains its origin until delayed name removal.
        CounterQueryCase {
            query: "rate(sum_over_time((metric_total{env=\"1\"}+metric_total{env=\"1\"})[30s:10s])[50s:10s])",
            intercept: 6.0,
            slope: 0.0,
            annotation_column: None,
        },
        CounterQueryCase {
            query: "rate(sum_over_time((metric_total{env=\"1\"}*2)[30s:10s])[50s:10s])",
            intercept: 6.0,
            slope: 0.0,
            annotation_column: Some(1),
        },
    ];
    for enabled in [false, true] {
        let engine = PromqlEngine::new(
            Arc::clone(&store),
            EngineOpts {
                enable_type_and_unit_labels: enabled,
                ..EngineOpts::default()
            },
        );
        for case in &cases {
            let query = case.query;
            let expected_infos = if enabled {
                case.annotation_column.map(|column| format!(
                    "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric_total\" (1:{column})"
                )).into_iter().collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let annotated = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 120_000)
                .await
                .unwrap();
            let annotations = annotated.annotations;
            let instant = annotated.result;
            assert2::assert!(annotations.infos == expected_infos);
            assert2::assert!(annotations.warnings.is_empty());
            check_instant_ledger(&instant, case.intercept + case.slope * 120.0);
            if query.starts_with("label_") {
                let QueryResult::InstantVector(samples) = &instant else {
                    panic!("instant vector")
                };
                assert2::assert!(samples[0].labels.get("name") == Some("metric_total"));
            }

            let expr =
                parse_promql_with_duration_context(query, DurationExprContext::instant(120_000))
                    .unwrap();
            for planner in [false, true] {
                let mut annotated = ANNOTATION_SOURCE
                    .scope(
                        query.to_owned(),
                        ANNOTATIONS.scope(RefCell::new(Annotations::new()), async {
                            let result = if planner {
                                let plan = engine
                                    .plan_instant_expr("t", &expr, 120_000)
                                    .await
                                    .unwrap()
                                    .expect("planner must claim composed query");
                                engine
                                    .assemble_planned_instant(plan, 120_000)
                                    .await
                                    .unwrap()
                            } else {
                                engine.eval_instant_expr("t", &expr, 120_000).await.unwrap()
                            };
                            crate::AnnotatedQueryResult {
                                result,
                                annotations: ANNOTATIONS.with(|sink| sink.borrow().clone()),
                            }
                        }),
                    )
                    .await;
                super::super::result_utils::finalize_metric_names(&mut annotated.result).unwrap();
                check_instant_ledger(&annotated.result, case.intercept + case.slope * 120.0);
                assert2::assert!(annotated.annotations.infos == expected_infos);
                assert2::assert!(annotated.annotations.warnings.is_empty());
            }
            let annotated = engine
                .query_range_with_annotations(&tenant_id("t"), query, 120_000, 140_000, secs(10))
                .await
                .unwrap();
            assert2::assert!(annotated.annotations.infos == expected_infos);
            assert2::assert!(annotated.annotations.warnings.is_empty());
            let QueryResult::RangeMatrix(series) = annotated.result else {
                panic!("range matrix")
            };
            assert2::assert!(series.len() == 1);
            assert2::assert!(series[0].labels.get("__name__").is_none());
            assert2::assert!(!series[0].drop_name);
            assert2::assert!(series[0].samples.len() == 3);
            for index in 0..series[0].samples.len() {
                let sample = &series[0].samples[index];
                let timestamp = sample.timestamp_ms;
                let value = &sample.value;
                let expected_timestamp = 120_000 + i64::try_from(index).unwrap() * 10_000;
                let SampleValue::Float(value) = value else {
                    panic!("float ledger")
                };
                assert2::assert!(timestamp == expected_timestamp);
                assert2::assert!(
                    (*value
                        - (case.intercept
                            + case.slope * f64::from(i32::try_from(expected_timestamp).unwrap())
                                / 1_000.0))
                        .abs()
                        < 1e-12
                );
            }
        }
        // Name-preserving outer folds must still respect the inner fold's
        // pending removal; a genuine last_over_time input keeps its name.
        for case in [
            NameRetentionCase {
                query: "last_over_time(sum_over_time(metric_total{env=\"1\"}[30s:10s])[50s:10s])",
                metric_name: None,
            },
            NameRetentionCase {
                query: "last_over_time(metric_total{env=\"1\"}[50s:10s])",
                metric_name: Some("metric_total"),
            },
        ] {
            let QueryResult::InstantVector(samples) = engine
                .query_instant(&tenant_id("t"), case.query, 120_000)
                .await
                .unwrap()
            else {
                panic!("instant vector")
            };
            assert2::assert!(samples.len() == 1);
            assert2::assert!(samples[0].labels.get("__name__") == case.metric_name);
        }
    }
}

fn check_instant_ledger(result: &QueryResult, expected: f64) {
    let QueryResult::InstantVector(samples) = result else {
        panic!("instant vector")
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("__name__").is_none());
    assert2::assert!(!samples[0].drop_name);
    let SampleValue::Float(value) = &samples[0].value else {
        panic!("float ledger")
    };
    assert2::assert!((value - expected).abs() < 1e-12);
}

#[tokio::test]
async fn delayed_name_removal_merges_disjoint_range_points_and_rejects_collisions() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "t",
        labels(&[crate::test_support::TestLabelPair::new(
            "__name__", "metric_a",
        )]),
        0,
        1.0,
    );
    store.push_float(
        "t",
        labels(&[crate::test_support::TestLabelPair::new(
            "__name__", "metric_b",
        )]),
        0,
        3.0,
    );
    store.push_float(
        "t",
        labels(&[crate::test_support::TestLabelPair::new(
            "__name__", "metric_b",
        )]),
        600_000,
        4.0,
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_range(
            &tenant_id("t"),
            "-metric_a or -metric_b",
            0,
            1_200_000,
            secs(600),
        )
        .await
        .unwrap();
    let QueryResult::RangeMatrix(series) = result else {
        panic!("range matrix")
    };
    assert2::assert!(series.len() == 1);
    assert2::assert!(series[0].labels.is_empty());
    assert2::assert!(!series[0].drop_name);
    assert2::assert!(
        series[0].samples
            == vec![
                crate::RangeSeriesSample::new(0, SampleValue::Float(-1.0)),
                crate::RangeSeriesSample::new(600_000, SampleValue::Float(-4.0))
            ]
    );
    let collision = "-{__name__=~\"metric_[ab]\"}";
    for result in [
        engine.query_instant(&tenant_id("t"), collision, 0).await,
        engine
            .query_range(&tenant_id("t"), collision, 0, 600_000, secs(600))
            .await,
    ] {
        assert2::assert!(
            matches!(result, Err(PromqlError::Exec(ref message)) if message.contains("same labelset"))
        );
    }
}
