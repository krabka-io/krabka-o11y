use super::{Arc, Mutex, Registry};

/// Shared registry that owns every metric this service emits.
///
/// It is wrapped in `Arc<Mutex<…>>` because `prometheus-client` needs
/// `&mut Registry` to register, and the exporter takes a read lock at scrape
/// time.
pub type SharedRegistry = Arc<Mutex<Registry>>;
