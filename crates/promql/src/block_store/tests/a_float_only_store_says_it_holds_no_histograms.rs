use super::*;

#[tokio::test]
pub(crate) async fn a_float_only_store_says_it_holds_no_histograms() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut floats = BlockStore::new(Arc::clone(&object_store), base.clone());
    let series = labels(&[("__name__", "up"), ("job", "api")]);
    write_float_block(
        &mut floats,
        "metrics/float/0001.parquet",
        &series,
        1_000,
        1.0,
    )
    .await;
    let matchers = [krabka_blockstore::LabelMatcher {
        name: "__name__".to_string(),
        op: krabka_blockstore::MatchOp::Eq,
        value: "up".to_string(),
    }];

    // `None` is a store built without a histogram block store, and an empty
    // histogram block store is the one `from_compaction_manifests` builds when
    // no manifest names a histogram block.
    for (case, store) in [
        ("no histogram store", MetricBlockStore::new(floats.clone())),
        (
            "an empty histogram store",
            MetricBlockStore::with_histograms(floats, BlockStore::new(object_store, base)),
        ),
    ] {
        check!(
            !store
                .may_have_histograms("tenant-a", &matchers, 0, 10_000)
                .await
                .unwrap(),
            "{case}"
        );
    }
}
