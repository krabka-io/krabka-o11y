use super::*;

#[tokio::test]
pub(crate) async fn instant_offset_and_at_selectors_read_the_right_blocks() {
    // One block per minute from 0s to 600s, and each sample's value is its
    // timestamp in seconds.
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store, base);
    let series = labels(&[("__name__", "up"), ("job", "api")]);
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
    let job = labels(&[("job", "api")]);

    // The same matcher set appears with different windows in one query, so
    // the per-query scan cache must not answer one window with another.
    for (query, expected_labels, expected_value) in [
        ("up", series.clone(), 600.0),
        ("up offset 5m", series.clone(), 300.0),
        ("up @ 120", series.clone(), 120.0),
        ("up - up offset 5m", job.clone(), 300.0),
        ("up @ 120 + up offset 5m", job.clone(), 420.0),
        ("count_over_time(up[2m])", job.clone(), 2.0),
        ("count_over_time(up[2m] offset 5m)", job.clone(), 2.0),
        ("sum_over_time(up[2m] @ 120)", job.clone(), 180.0),
        (
            "sum_over_time(up[2m]) - sum_over_time(up[2m] offset 5m)",
            job.clone(),
            600.0,
        ),
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), query, 600_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("{query}: expected an instant vector");
        };
        let samples = samples
            .into_iter()
            .map(|sample| (sample.labels, sample.ts_ms, sample.value))
            .collect::<Vec<_>>();
        check!(
            samples
                == vec![(
                    crate::PromqlLabels::from(expected_labels),
                    600_000,
                    SampleValue::Float(expected_value)
                )],
            "{query}"
        );
    }
}
