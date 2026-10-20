use std::{sync::atomic::Ordering, time::Instant};

use krabka_units::prelude::*;

use super::*;

#[tokio::test]
async fn residual_range_queries_read_persisted_blocks_once() {
    let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer = BlockStore::new(Arc::clone(&backing), base.clone());
    let series = up_api_labels();
    let mut manifests = Vec::new();
    for block in 0..6 {
        let rows = (block * 30..(block + 1) * 30)
            .map(|minute| {
                float_sample_row(
                    series.fingerprint(),
                    i64::from(minute) * 60_000,
                    f64::from(minute),
                    None,
                )
            })
            .collect::<Vec<_>>();
        manifests.push(
            sidecar_manifest(
                &writer,
                MetricBlockKind::Float,
                &format!("metrics/float/{block}.parquet"),
                float_sample_schema(),
                encode_float_samples(&rows).unwrap(),
                &series,
            )
            .await,
        );
    }

    for query in [
        "deriv(up[5m])",
        "deriv(up[1h])",
        "deriv(up[5m] offset 10m)",
        "deriv(up[1h] offset 10m)",
        "deriv(up[1h] offset -10m)",
    ] {
        let counting = CountingObjectStore::wrap(Arc::clone(&backing));
        let store = MetricBlockStore::from_compaction_manifests(
            BlockStore::new(Arc::clone(&counting.store), base.clone()),
            None,
            &manifests,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let started = Instant::now();
        let actual = engine
            .query_range_with_annotations(
                &tenant_id("tenant-a"),
                query,
                7_200_000,
                9_000_000,
                millis(60_000),
            )
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let requests = *counting.counts.lock().unwrap();
        let bytes = counting.fetched_bytes.load(Ordering::Relaxed);
        let QueryResult::RangeMatrix(matrix) = &actual.result else {
            panic!("expected a range matrix");
        };
        assert2::assert!(matrix.len() == 1);
        assert2::assert!(matrix[0].samples.len() == 31);
        for sample in &matrix[0].samples {
            let SampleValue::Float(value) = sample.value else {
                panic!("expected a float");
            };
            assert2::assert!((value - 1.0 / 60.0).abs() < 1e-12);
        }
        assert2::assert!(requests.full_gets == 0);
        // Six blocks are an upper bound. Each cold block needs its footer,
        // page index and columns, independently of the 31 evaluation steps.
        assert2::assert!(requests.heads <= 6);
        assert2::assert!(requests.range_gets <= 18);
        assert2::assert!(bytes > 0);
        println!("{query}: range {elapsed:?}, requests={requests:?}, fetched_bytes={bytes}");

        let before = *counting.counts.lock().unwrap();
        let bytes_before = counting.fetched_bytes.load(Ordering::Relaxed);
        let started = Instant::now();
        for minute in 120..=150 {
            let instant = engine
                .query_instant_with_annotations(&tenant_id("tenant-a"), query, minute * 60_000)
                .await
                .unwrap();
            assert2::assert!(instant.annotations == actual.annotations);
        }
        let elapsed = started.elapsed();
        let uncached = counting.counts.lock().unwrap().since(before);
        let uncached_bytes = counting.fetched_bytes.load(Ordering::Relaxed) - bytes_before;
        assert2::assert!(requests.heads < uncached.heads);
        assert2::assert!(bytes < uncached_bytes);
        println!(
            "{query}: instant loop {elapsed:?}, requests={uncached:?}, fetched_bytes={uncached_bytes}"
        );
    }
}
