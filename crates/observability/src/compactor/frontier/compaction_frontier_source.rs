use super::{CompactionFrontier, SharedCompactionFrontier};

#[derive(Clone, Debug)]
pub(crate) enum CompactionFrontierSource {
    Snapshot(CompactionFrontier),
    Shared(SharedCompactionFrontier),
}

impl CompactionFrontierSource {
    pub(crate) fn snapshot(&self) -> CompactionFrontier {
        match self {
            Self::Snapshot(frontier) => frontier.clone(),
            Self::Shared(frontier) => frontier.snapshot(),
        }
    }
}
