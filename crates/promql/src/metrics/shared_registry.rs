use super::{Arc, Mutex, Registry};

/// Shared registry that owns every metric this process emits. It is in an
/// `Arc<Mutex<…>>`, because `prometheus-client` needs `&mut Registry` to
/// register a metric and the exporter needs shared read access at scrape time.
pub type SharedRegistry = Arc<Mutex<Registry>>;
