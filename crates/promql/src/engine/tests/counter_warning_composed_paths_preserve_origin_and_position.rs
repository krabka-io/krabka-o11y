use std::cell::RefCell;

use super::{
    super::annotations::{ANNOTATION_SOURCE, ANNOTATIONS},
    *,
};
use crate::{Annotations, DurationExprContext, parse_promql_with_duration_context};

#[tokio::test]
async fn counter_warning_composed_paths_preserve_origin_and_position() {
    let mut store = InMemoryMetricStore::new();
    for seconds in (0..=140).step_by(10) {
        for (metric, env, scale, metric_type) in [
            ("metric_total", "1", 1.0, None),
            ("metric_total", "2", 2.0, None),
            ("typed_total", "typed", 1.0, Some("counter")),
        ] {
            let mut labels = labels(&[("__name__", metric), ("env", env)]);
            if let Some(metric_type) = metric_type {
                labels.insert("__type__", metric_type);
            }
            store.push_float("t", labels, seconds * 1_000, seconds as f64 * scale);
        }
    }
    let store = Arc::new(store);
    // Independent input ledger: metric_total/env1 rises by 10 per ten seconds,
    // env2 by 20. A three-point rolling sum rises by 30 per ten seconds.
    let cases = [
        ("rate({env=\"1\"}[1m])", 1.0, 0.0, Some(6)),
        ("increase({env=\"1\"}[1m])", 60.0, 0.0, Some(10)),
        ("sum(rate({env=\"1\"}[1m])) by (env)", 1.0, 0.0, Some(10)),
        (
            "label_join(rate({env=\"1\"}[1m]),\"name\",\"_\",\"__name__\")",
            1.0,
            0.0,
            Some(17),
        ),
        (
            "label_replace(rate({env=\"1\"}[1m]),\"name\",\"$1\",\"__name__\",\"(.+)\")",
            1.0,
            0.0,
            Some(20),
        ),
        (
            "sum by (__name__) (rate(metric_total{env=\"1\"}[1m]) or metric_total{env=\"2\"})",
            1.0,
            2.0,
            Some(25),
        ),
        (
            "rate(sum_over_time(metric_total{env=\"1\"}[30s:10s])[50s:10s])",
            3.0,
            0.0,
            Some(1),
        ),
        (
            "increase(sum_over_time(metric_total{env=\"1\"}[30s:10s])[50s:10s])",
            150.0,
            0.0,
            Some(1),
        ),
        (
            "rate(last_over_time(metric_total{env=\"1\"}[30s:10s])[50s:10s])",
            1.0,
            0.0,
            Some(1),
        ),
        ("rate(typed_total[1m])", 1.0, 0.0, None),
        // Vector/vector arithmetic changes metric schema eagerly; scalar
        // arithmetic retains its origin until delayed name removal.
        (
            "rate(sum_over_time((metric_total{env=\"1\"}+metric_total{env=\"1\"})[30s:10s])[50s:10s])",
            6.0,
            0.0,
            None,
        ),
        (
            "rate(sum_over_time((metric_total{env=\"1\"}*2)[30s:10s])[50s:10s])",
            6.0,
            0.0,
            Some(1),
        ),
    ];
    for enabled in [false, true] {
        let engine = PromqlEngine::new(
            Arc::clone(&store),
            EngineOpts {
                enable_type_and_unit_labels: enabled,
                ..EngineOpts::default()
            },
        );
        for (query, intercept, slope, column) in cases {
            let expected_infos = if enabled {
                column.map(|column| format!(
                    "PromQL info: metric might not be a counter, __type__ label is not set to \"counter\" or \"histogram\", got \"\": \"metric_total\" (1:{column})"
                )).into_iter().collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            let (instant, annotations) = engine
                .query_instant_with_annotations(&tenant_id("t"), query, 120_000)
                .await
                .unwrap();
            assert2::assert!(annotations.infos == expected_infos);
            assert2::assert!(annotations.warnings.is_empty());
            check_instant_ledger(&instant, intercept + slope * 120.0);
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
                let (mut result, annotations) = ANNOTATION_SOURCE
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
                            (result, ANNOTATIONS.with(|sink| sink.borrow().clone()))
                        }),
                    )
                    .await;
                super::super::result_utils::finalize_metric_names(&mut result).unwrap();
                check_instant_ledger(&result, intercept + slope * 120.0);
                assert2::assert!(annotations.infos == expected_infos);
                assert2::assert!(annotations.warnings.is_empty());
            }
            let (result, annotations) = engine
                .query_range_with_annotations(&tenant_id("t"), query, 120_000, 140_000, secs(10))
                .await
                .unwrap();
            assert2::assert!(annotations.infos == expected_infos);
            assert2::assert!(annotations.warnings.is_empty());
            let QueryResult::RangeMatrix(series) = result else {
                panic!("range matrix")
            };
            assert2::assert!(series.len() == 1);
            assert2::assert!(series[0].labels.get("__name__").is_none());
            assert2::assert!(!series[0].drop_name);
            assert2::assert!(series[0].samples.len() == 3);
            for (index, (timestamp, value)) in series[0].samples.iter().enumerate() {
                let expected_timestamp = 120_000 + index as i64 * 10_000;
                let SampleValue::Float(value) = value else {
                    panic!("float ledger")
                };
                assert2::assert!(*timestamp == expected_timestamp);
                assert2::assert!(
                    (*value - (intercept + slope * expected_timestamp as f64 / 1_000.0)).abs()
                        < 1e-12
                );
            }
        }
        // Name-preserving outer folds must still respect the inner fold's
        // pending removal; a genuine last_over_time input keeps its name.
        for (query, name) in [
            (
                "last_over_time(sum_over_time(metric_total{env=\"1\"}[30s:10s])[50s:10s])",
                None,
            ),
            (
                "last_over_time(metric_total{env=\"1\"}[50s:10s])",
                Some("metric_total"),
            ),
        ] {
            let QueryResult::InstantVector(samples) = engine
                .query_instant(&tenant_id("t"), query, 120_000)
                .await
                .unwrap()
            else {
                panic!("instant vector")
            };
            assert2::assert!(samples.len() == 1);
            assert2::assert!(samples[0].labels.get("__name__") == name);
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
    store.push_float("t", labels(&[("__name__", "metric_a")]), 0, 1.0);
    store.push_float("t", labels(&[("__name__", "metric_b")]), 0, 3.0);
    store.push_float("t", labels(&[("__name__", "metric_b")]), 600_000, 4.0);
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
                (0, SampleValue::Float(-1.0)),
                (600_000, SampleValue::Float(-4.0))
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
