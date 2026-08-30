use super::{Arc, ArcSwap, TraceIndex};

/// A `TraceIndex` shared between the span store and the live sources.
///
/// It is swappable at runtime, so a background task can reload it without a
/// restart.
pub type SharedTraceIndex = Arc<ArcSwap<TraceIndex>>;
