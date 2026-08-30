use super::*;

/// A dynamic tenant index is built only when the querier serves every
/// tenant *and* was pointed at a tenant index source. Either condition
/// alone leaves it reading its own index.
#[tokio::test]
pub(crate) async fn a_dynamic_tenant_index_needs_both_no_tenant_and_a_tenant_index_source() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_log_index_manifest(dir.path(), &LabelIndex::default(), &BlockIndex::default())
        .expect("seed an empty local manifest");
    let configured = ConfiguredObjectStore {
        store: Arc::new(object_store::memory::InMemory::new()),
        prefix: ObjectPath::from("observability"),
    };
    write_tenant_log_index_manifest_to_object_store(
        configured.store.as_ref(),
        &ObjectPath::from("observability/index"),
        "tenant-a",
        &LabelIndex::default(),
        &BlockIndex::default(),
    )
    .await
    .expect("seed an empty tenant manifest");
    for (tenant, source, dynamic) in [
        (None, QuerierIndexSource::TenantObjectStoreManifest, true),
        (None, QuerierIndexSource::TenantObjectStoreShards, true),
        (None, QuerierIndexSource::LocalManifest, false),
        (
            Some("tenant-a"),
            QuerierIndexSource::TenantObjectStoreManifest,
            false,
        ),
    ] {
        let config = ServiceConfig {
            tenant: tenant.map(str::to_owned),
            querier_index_source: source,
            index_prefix: Some("index".to_owned()),
            data_root: dir.path().to_path_buf(),
            ..ServiceConfig::default()
        };
        let state = build_configured_querier_state(&config, &configured)
            .await
            .expect("the configuration is valid");
        check!(
            state.dynamic_index.is_some() == dynamic,
            "{tenant:?} with {source:?}"
        );
    }
}
