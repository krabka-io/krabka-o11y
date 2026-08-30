use super::*;

/// A `TenantObjectStoreManifest` source backed by an empty in-memory
/// store, with no manifest present, must return Ok with an empty
/// self-clone index. It must not propagate `NotFound` as an error.
#[tokio::test]
pub(crate) async fn querier_state_with_request_tenant_index_tolerates_absent_manifest() {
    use object_store::memory::InMemory;

    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = ObjectPath::default();

    let state = QuerierState::new(".", LabelIndex::default(), BlockIndex::default())
        .with_dynamic_tenant_object_store_manifest(store, prefix);

    let query_range = TimeRange::new(0, 1).unwrap();
    let result = state
        .with_request_tenant_index("test-tenant", query_range)
        .await;

    assert!(
        result.is_ok(),
        "expected Ok on absent cold index manifest, got: {:?}",
        result.err()
    );
    let returned = result.unwrap();
    assert!(
        returned.block_index.blocks().is_empty(),
        "expected empty block index when no manifest exists"
    );
}
