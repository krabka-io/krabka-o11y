use super::{Arc, HttpConfig, Router, SpanStore, TraceqlEngine, router_with_config};

pub fn router<S>(engine: Arc<TraceqlEngine<S>>) -> Router
where
    S: SpanStore + 'static,
{
    router_with_config(engine, HttpConfig::default())
}
