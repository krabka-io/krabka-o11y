use super::*;

/// A querier that serves every tenant from a tenant index source takes a
/// branch of its own, because it reads its index per request rather than
/// once at start-up. That branch used to return a bare state, so the
/// deployment that actually serves several tenants was the one deployment
/// with no query limits and no runtime policy at all.
#[tokio::test]
pub(crate) async fn a_tenant_less_dynamic_index_querier_still_carries_its_limits() {
    let configured = ConfiguredObjectStore {
        store: Arc::new(object_store::memory::InMemory::new()),
        prefix: ObjectPath::from("observability"),
    };
    let overrides = Arc::new(
        OverridesProvider::from_yaml_over(
            "overrides:\n  tenant-capped:\n    max_query_series: 3\n",
            &Limits {
                max_query_series: 99,
                ..Limits::default()
            },
        )
        .expect("the overrides file parses"),
    );

    for source in [
        QuerierIndexSource::TenantObjectStoreManifest,
        QuerierIndexSource::TenantObjectStoreShards,
    ] {
        let config = ServiceConfig {
            tenant: None,
            querier_index_source: source,
            index_prefix: Some("index".to_owned()),
            querier_cold_block_fetch_concurrency: std::num::NonZeroUsize::new(3)
                .expect("a nonzero concurrency"),
            ..ServiceConfig::default()
        };
        let state = build_configured_querier_state(&config, &configured, Arc::clone(&overrides))
            .await
            .expect("the configuration is valid");

        check!(state.dynamic_index.is_some(), "{source:?}");
        check!(
            state.limits.max_query_series == 99,
            "the provider's defaults reach this branch: {source:?}"
        );
        check!(
            state
                .with_tenant_limits(&TenantId::new("tenant-capped").expect("a valid tenant id"))
                .limits
                .max_query_series
                == 3,
            "and a per-tenant override resolves through it: {source:?}"
        );
        check!(
            state.cold_block_fetch_concurrency.get() == 3,
            "the runtime policy reaches this branch too: {source:?}"
        );
    }
}
