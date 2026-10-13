use super::*;

#[tokio::test]
pub(crate) async fn a_float_only_store_says_it_holds_no_histograms() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut floats = BlockStore::new(Arc::clone(&object_store), base.clone());
    let series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    write_float_block(
        &mut floats,
        "metrics/float/0001.parquet",
        &series,
        1_000,
        1.0,
    )
    .await;
    let matchers = [crate::PromqlMatcher {
        name: "__name__".to_string(),
        op: krabka_blockstore::MatchOp::Eq,
        value: "up".to_string().into(),
    }];

    // `None` is a store built without a histogram block store, and an empty
    // histogram block store is the one `from_compaction_manifests` builds when
    // no manifest names a histogram block.
    struct StoreCase {
        description: &'static str,
        store: MetricBlockStore,
    }
    for case in [
        StoreCase {
            description: "no histogram store",
            store: MetricBlockStore::new(floats.clone()),
        },
        StoreCase {
            description: "an empty histogram store",
            store: MetricBlockStore::with_histograms(floats, BlockStore::new(object_store, base)),
        },
    ] {
        check!(
            !case
                .store
                .may_have_histograms("tenant-a", &matchers, 0, 10_000)
                .await
                .unwrap(),
            "{}",
            case.description
        );
    }
}
