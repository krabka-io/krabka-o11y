use super::{ScopedTag, Metrics};

/// The partial result of one tag-names job.
#[derive(Clone, Debug, Default)]
pub struct TagNamesPartial {
    pub tags: Vec<ScopedTag>,
    pub metrics: Metrics,
}
