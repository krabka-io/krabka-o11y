use super::{InstrumentationKey, SpanRef};

pub(crate) type InstrumentationGroups<'a> = Vec<InstrumentationGroup<'a>>;

pub(crate) struct InstrumentationGroup<'a> {
    pub(crate) key: InstrumentationKey,
    pub(crate) spans: Vec<&'a SpanRef>,
}
