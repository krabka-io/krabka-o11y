use super::*;

/// The samples and annotations of one instant query.
pub(crate) struct InstantUpQuery {
    pub(crate) samples: Vec<InstantSample>,
    pub(crate) annotations: Annotations,
}

/// Runs the instant query `up` for `tenant-a` at `t = 1s` over `block_store`.
pub(crate) async fn instant_up_query(block_store: BlockStore) -> InstantUpQuery {
    let store = MetricBlockStore::new(block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let (result, annotations) = engine
        .query_instant_with_annotations(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    InstantUpQuery {
        samples,
        annotations,
    }
}
