use super::*;

#[tokio::test]
async fn byte_strings_keep_identity_through_composed_queries() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "t",
        labels(&[("__name__", "g"), ("src", "first\nsecond")]),
        10_000,
        -2.0,
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    async fn evaluate(
        engine: &PromqlEngine<InMemoryMetricStore>,
        query: &str,
        planned: bool,
    ) -> Result<QueryResult, PromqlError> {
        if planned {
            engine.query_instant(&tenant_id("t"), query, 10_000).await
        } else {
            let expr = crate::parse_promql_with_duration_context(
                query,
                crate::DurationExprContext::instant(10_000),
            )?;
            engine.eval_instant_expr("t", &expr, 10_000).await
        }
    }
    for planned in [false, true] {
        for (query, expected) in [
            (r#""\xff""#, vec![0xff]),
            (r#""\xfe""#, vec![0xfe]),
            (r#""\xe2\x82""#, vec![0xe2, 0x82]),
            (r#""\xef\xbf\xbd""#, vec![0xef, 0xbf, 0xbd]),
        ] {
            let QueryResult::Str { value, .. } = evaluate(&engine, query, planned).await.unwrap()
            else {
                panic!("string expected")
            };
            assert2::assert!(value.as_bytes() == expected.as_slice(), "{query}");
        }
        let replace =
            |replacement: &str| format!(r#"label_replace(g,"raw","{replacement}","src","(.*)")"#);
        let left = replace(r"\xff");
        let right = replace(r"\xfe");
        for query in [
            format!("sum by(raw) (abs({left} or {right}))"),
            format!("sum by(raw) (sum_over_time((abs({left} or {right}))[10s:5s]))"),
        ] {
            let QueryResult::InstantVector(samples) =
                evaluate(&engine, &query, planned).await.unwrap()
            else {
                panic!("vector expected")
            };
            let mut identities = samples
                .iter()
                .map(|sample| sample.labels.get_value("raw").unwrap().as_bytes().to_vec())
                .collect::<Vec<_>>();
            identities.sort();
            assert2::assert!(identities == vec![vec![0xfe], vec![0xff]], "{query}");
            assert2::assert!(
                samples
                    .iter()
                    .all(|sample| float_value(&sample.value) == 2.0),
                "{query}"
            );
        }
        let query = format!(
            r#"label_join(label_replace({left},"copy","$1","raw","(.*)"),"joined","\xfe","raw","copy")"#
        );
        let QueryResult::InstantVector(samples) = evaluate(&engine, &query, planned).await.unwrap()
        else {
            panic!("vector expected")
        };
        assert2::assert!(samples.len() == 1);
        assert2::assert!(
            samples[0].labels.get_value("joined").unwrap().as_bytes() == &[0xff, 0xfe, 0xff]
        );
        // Go regexp matches newlines in label_replace's anchored dot expression.
        assert2::assert!(samples[0].labels.get_value("raw").unwrap().as_bytes() == &[0xff]);
        for query in [
            r#"count_values("\xff",g)"#,
            r#"label_replace(g,"\xff","x","src",".*")"#,
            r#"label_replace(g,"raw","x","src","\xff")"#,
        ] {
            assert2::assert!(evaluate(&engine, query, planned).await.is_err(), "{query}");
        }
    }
}

#[tokio::test]
async fn stored_byte_labels_survive_instant_range_aggregation_and_rate_planners() {
    let identities = [
        vec![0xff],
        vec![0xfe],
        "�".as_bytes().to_vec(),
        b"__krabka_bytes_ff".to_vec(),
    ];
    let mut store = InMemoryMetricStore::new();
    for (index, raw) in identities.iter().enumerate() {
        let mut labels = crate::PromqlLabels::from_pairs([("__name__", "raw_total")]);
        labels.insert("raw", crate::PromqlString::from(raw.clone()));
        let base = f64::from(u32::try_from(index + 1).unwrap());
        for (time, multiplier) in [(0, 0.0), (10_000, 1.0), (20_000, 2.0)] {
            store.push_float("t", labels.clone(), time, base * multiplier);
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for planned in [false, true] {
        for (query, multiplier) in [
            ("raw_total", 2.0),
            ("abs(raw_total)", 2.0),
            ("sum by(raw)(raw_total)", 2.0),
            ("sum_over_time(raw_total[20s])", 3.0),
            ("rate(raw_total[20s])", 0.1),
            ("sum by(raw)(rate(raw_total[20s]))", 0.1),
        ] {
            let result = if planned {
                engine.query_instant(&tenant_id("t"), query, 20_000).await
            } else {
                let expr = crate::parse_promql_with_duration_context(
                    query,
                    crate::DurationExprContext::instant(20_000),
                )
                .unwrap();
                engine.eval_instant_expr("t", &expr, 20_000).await
            }
            .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("expected vector")
            };
            assert2::assert!(samples.len() == 4, "{query}, planned={planned}");
            for (index, raw) in identities.iter().enumerate() {
                let sample = samples
                    .iter()
                    .find(|sample| sample.labels.get_value("raw").unwrap().as_bytes() == raw)
                    .unwrap();
                let expected = f64::from(u32::try_from(index + 1).unwrap()) * multiplier;
                assert2::assert!(
                    (float_value(&sample.value) - expected).abs() < 1e-12,
                    "{query}, raw={raw:?}"
                );
            }
        }
    }
    for forced in [false, true] {
        let result = if forced {
            engine
                .eval_range_via_planner_forced("t", "raw_total", 10_000, 20_000, secs(10))
                .await
        } else {
            engine
                .query_range(&tenant_id("t"), "raw_total", 10_000, 20_000, secs(10))
                .await
        }
        .unwrap();
        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix")
        };
        assert2::assert!(series.len() == 4);
        for (index, raw) in identities.iter().enumerate() {
            let actual = series
                .iter()
                .find(|series| series.labels.get_value("raw").unwrap().as_bytes() == raw)
                .unwrap();
            let base = f64::from(u32::try_from(index + 1).unwrap());
            assert2::assert!(
                actual.samples
                    == [
                        (10_000, SampleValue::Float(base)),
                        (20_000, SampleValue::Float(base * 2.0))
                    ]
            );
        }
    }
}
