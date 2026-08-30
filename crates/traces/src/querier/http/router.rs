use super::{SpanStore, Arc, TraceqlEngine, Router, router_with_config, HttpConfig};

pub fn router<S>(engine: Arc<TraceqlEngine<S>>) -> Router
where
    S: SpanStore + 'static,
{
    router_with_config(engine, HttpConfig::default())
}
