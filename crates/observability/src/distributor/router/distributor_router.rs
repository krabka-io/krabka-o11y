use super::{
    AllowAllIngestLimiter, Arc, Limits, LogWalSink, OverridesProvider, Router, ServiceMetrics,
    distributor_router_with_sink,
};

/// A push router with every limit turned off.
///
/// It takes a sink and nothing else, so there is no operator configuration for
/// it to read a limit from. Use
/// [`distributor_router_with_overrides`](super::distributor_router_with_overrides)
/// to give it a tenant's limits.
pub fn distributor_router(sink: impl LogWalSink) -> Router {
    distributor_router_with_overrides(sink, OverridesProvider::new(Limits::unenforced()))
}

/// A push router that resolves each tenant's limits through `overrides`.
pub fn distributor_router_with_overrides(
    sink: impl LogWalSink,
    overrides: OverridesProvider,
) -> Router {
    distributor_router_with_sink(
        Arc::new(sink),
        Arc::new(AllowAllIngestLimiter),
        Arc::new(overrides),
        None,
        ServiceMetrics::new(),
    )
}
