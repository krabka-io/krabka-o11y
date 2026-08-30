use super::{Arc, Mutex, Registry};

/// Shared registry that owns every metric the service emits.
///
/// The type is `Arc<Mutex<…>>` because `prometheus-client` needs
/// `&mut Registry` to register a metric, and the `/metrics` exporter needs
/// shared read access.
pub type SharedRegistry = Arc<Mutex<Registry>>;
