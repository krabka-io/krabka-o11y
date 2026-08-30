use super::{InstrumentationKey, SpanRef};

pub(crate) type InstrumentationGroups<'a> = Vec<(InstrumentationKey, Vec<&'a SpanRef>)>;
