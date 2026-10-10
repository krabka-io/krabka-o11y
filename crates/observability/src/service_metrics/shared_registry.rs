use super::{Arc, Mutex, Registry};

/// Shared registry that owns every metric a service process emits.
///
/// It is wrapped in `Arc<Mutex<…>>` because `prometheus-client` needs
/// `&mut Registry` to register a metric, and the `/metrics` exporter needs
/// shared read access at scrape time.
pub type SharedRegistry = Arc<Mutex<Registry>>;
