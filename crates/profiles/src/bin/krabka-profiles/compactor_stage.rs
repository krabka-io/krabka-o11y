use super::{AllStage, Arc, Cli, ObjectStore, ServiceMetrics, compaction_loop};

/// The compactor, as `--target all` runs it.
///
/// Last in the stop order. Its pass is the longest single unit of work in the
/// process, and it is the only role whose work is pure rearrangement: a pass
/// abandoned halfway loses the pass and nothing else, because the index it
/// would have published is only written at the end. Everything ahead of it in
/// the order has something to lose instead.
pub(crate) fn compactor_stage(
    cli: &Arc<Cli>,
    store: Arc<dyn ObjectStore>,
    index_key: String,
    metrics: &ServiceMetrics,
) -> AllStage {
    let cli = Arc::clone(cli);
    let metrics = metrics.clone();
    Box::new(move |token| Box::pin(compaction_loop(cli, store, index_key, metrics, token)))
}
