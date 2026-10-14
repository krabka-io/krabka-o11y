use super::*;

struct SelectorExpectation {
    query: &'static str,
    labels: Labels,
    value: f64,
}

#[tokio::test]
pub(crate) async fn instant_offset_and_at_selectors_read_the_right_blocks() {
    // One block per minute from 0s to 600s, and each sample's value is its
    // timestamp in seconds.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store, base);
    let series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    for minute in 0..=10_i64 {
        let ts_ms = minute * 60_000;
        let value = f64::from(u32::try_from(minute * 60).unwrap());
        write_float_block(
            &mut block_store,
            &format!("metrics/float/{minute:04}.parquet"),
            &series,
            ts_ms,
            value,
        )
        .await;
    }
    let engine = PromqlEngine::new(
        Arc::new(MetricBlockStore::new(block_store)),
        EngineOpts::default(),
    );
    let job = labels(&[crate::test_support::TestLabelPair::new("job", "api")]);

    // The same matcher set appears with different windows in one query, so
    // the per-query scan cache must not answer one window with another.
    for case in [
        SelectorExpectation {
            query: "up",
            labels: series.clone(),
            value: 600.0,
        },
        SelectorExpectation {
            query: "up offset 5m",
            labels: series.clone(),
            value: 300.0,
        },
        SelectorExpectation {
            query: "up @ 120",
            labels: series.clone(),
            value: 120.0,
        },
        SelectorExpectation {
            query: "up - up offset 5m",
            labels: job.clone(),
            value: 300.0,
        },
        SelectorExpectation {
            query: "up @ 120 + up offset 5m",
            labels: job.clone(),
            value: 420.0,
        },
        SelectorExpectation {
            query: "count_over_time(up[2m])",
            labels: job.clone(),
            value: 2.0,
        },
        SelectorExpectation {
            query: "count_over_time(up[2m] offset 5m)",
            labels: job.clone(),
            value: 2.0,
        },
        SelectorExpectation {
            query: "sum_over_time(up[2m] @ 120)",
            labels: job.clone(),
            value: 180.0,
        },
        SelectorExpectation {
            query: "sum_over_time(up[2m]) - sum_over_time(up[2m] offset 5m)",
            labels: job.clone(),
            value: 600.0,
        },
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), case.query, 600_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("{}: expected an instant vector", case.query);
        };
        assert2::assert!(samples.len() == 1, "{}", case.query);
        let sample = &samples[0];
        check!(
            sample.labels == crate::PromqlLabels::from(case.labels)
                && sample.ts_ms == 600_000
                && sample.value == SampleValue::Float(case.value),
            "{}",
            case.query
        );
    }
}
