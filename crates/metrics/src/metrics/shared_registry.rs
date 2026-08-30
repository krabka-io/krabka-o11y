use super::{Arc, Mutex, Registry};

/// Shared registry that owns every metric this process emits. It is wrapped in
/// `Arc<Mutex<…>>`, because `prometheus-client` needs `&mut Registry` to
/// register and the exporter needs shared read access at scrape time.
pub type SharedRegistry = Arc<Mutex<Registry>>;
