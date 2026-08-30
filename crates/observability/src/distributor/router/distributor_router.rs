use super::{
    AllowAllIngestLimiter, Arc, LogWalSink, Router, ServiceMetrics, distributor_router_with_sink,
};

pub fn distributor_router(sink: impl LogWalSink) -> Router {
    distributor_router_with_sink(
        Arc::new(sink),
        Arc::new(AllowAllIngestLimiter),
        None,
        None,
        None,
        None,
        ServiceMetrics::new(),
    )
}
