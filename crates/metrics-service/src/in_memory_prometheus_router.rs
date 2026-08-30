use super::{InMemoryMetricStore, Router, prometheus_router_for_store};

pub fn in_memory_prometheus_router() -> Router {
    prometheus_router_for_store(InMemoryMetricStore::new())
}
