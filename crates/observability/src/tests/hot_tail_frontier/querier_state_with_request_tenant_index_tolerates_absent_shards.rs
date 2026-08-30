use super::*;

/// Same check for the `TenantObjectStoreShards` variant.
#[tokio::test]
pub(crate) async fn querier_state_with_request_tenant_index_tolerates_absent_shards() {
    use object_store::memory::InMemory;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = ObjectPath::default();

    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_dynamic_tenant_object_store_shards(store, prefix);

    let query_range = TimeRange::new(0, 1).unwrap();
    let result = state
        .with_request_tenant_index("test-tenant", query_range)
        .await;

    assert!(
        result.is_ok(),
        "expected Ok on absent cold index shards, got: {:?}",
        result.err()
    );
}
