use super::*;

#[derive(Clone)]
pub(crate) struct HotTailDependency {
    pub(crate) source: Arc<dyn LogHotTail>,
    pub(crate) frontier: CompactionFrontierSource,
}
