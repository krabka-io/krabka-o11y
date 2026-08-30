use super::{Arc, CompactionFrontierSource, LogHotTail};

#[derive(Clone)]
pub(crate) struct HotTailDependency {
    pub(crate) source: Arc<dyn LogHotTail>,
    pub(crate) frontier: CompactionFrontierSource,
}
