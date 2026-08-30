use super::*;

#[derive(Clone)]
pub(crate) struct HotTailState {
    pub(crate) source: Arc<dyn LogHotTail>,
    pub(crate) frontier: CompactionFrontierSource,
}
