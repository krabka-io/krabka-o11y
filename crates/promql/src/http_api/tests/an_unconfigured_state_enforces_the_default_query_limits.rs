use super::*;

/// Mimir applies its default limits without a runtime config, so a querier
/// started with no overrides file must still cap what one query selects. A
/// state built without `with_query_limits` enforces `Limits::default()` at the
/// HTTP gate and in the engine.
///
/// The engine options are process caps, as Mimir's `-querier.max-samples` is:
/// a tenant's cap lowers a process cap and never raises it, and a tenant cap of
/// zero leaves the process cap in force. Without that rule a default tenant
/// limit would silently override `--query-max-samples`.
#[test]
pub(crate) fn an_unconfigured_state_enforces_the_default_query_limits() {
    let tenant = TenantId::new("tenant-a").expect("a valid tenant id");
    let default_series = usize::try_from(Limits::default().max_fetched_series_per_query)
        .expect("the default cap fits a usize");
    let engine_caps = |process: EngineOpts, limits: Option<Limits>| {
        let state = PrometheusApiState::new(Arc::new(two_series_store()), process);
        let state = match limits {
            Some(limits) => state.with_query_limits(OverridesProvider::new(limits)),
            None => state,
        };
        let opts = state.engine_for_tenant(&tenant).opts;
        (opts.max_samples, opts.max_fetched_series)
    };

    let unconfigured = PrometheusApiState::new(Arc::new(two_series_store()), EngineOpts::default());
    assert2::assert!(enforce_selected_series_limit(&unconfigured, &tenant, default_series).is_ok());
    assert2::assert!(
        enforce_selected_series_limit(&unconfigured, &tenant, default_series + 1).is_err()
    );

    let process = EngineOpts {
        max_samples: 100,
        max_fetched_series: 7,
        ..EngineOpts::default()
    };
    let tenant_caps = |samples, series| Limits {
        max_samples_per_query: samples,
        max_fetched_series_per_query: series,
        ..Limits::default()
    };
    let cases = [
        (
            "default limits, no process series cap",
            EngineOpts::default(),
            None,
            (EngineOpts::default().max_samples, default_series),
        ),
        (
            "tenant caps lower the process caps",
            process,
            Some(tenant_caps(10, 3)),
            (10, 3),
        ),
        (
            "tenant caps cannot raise the process caps",
            process,
            Some(tenant_caps(1_000, 70)),
            (100, 7),
        ),
        (
            "zero tenant caps keep the process caps",
            process,
            Some(tenant_caps(0, 0)),
            (100, 7),
        ),
        (
            "a zero process series cap leaves the tenant cap",
            EngineOpts::default(),
            Some(tenant_caps(0, 5)),
            (EngineOpts::default().max_samples, 5),
        ),
    ];
    for (name, process, limits, expected) in cases {
        assert2::check!(engine_caps(process, limits) == expected, "{name}");
    }
}
