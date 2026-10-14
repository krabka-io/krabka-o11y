use super::*;

#[test]
fn ruler_tenants_lists_each_configured_tenant() {
    let state = PrometheusApiState::new(
        Arc::new(InMemoryMetricStore::new()),
        crate::EngineOpts::default(),
    );
    let mut rules = state.ruler_rules.write().expect("ruler rules lock");
    rules.insert(TenantId::new("tenant-b").unwrap(), BTreeMap::new());
    rules.insert(TenantId::new("tenant-a").unwrap(), BTreeMap::new());
    drop(rules);

    assert2::assert!(
        state
            .ruler_tenants()
            .iter()
            .map(TenantId::as_str)
            .collect::<Vec<_>>()
            == vec!["tenant-a", "tenant-b"]
    );
}
