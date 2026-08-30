use super::*;

pub(crate) type InstrumentationGroups<'a> = Vec<(InstrumentationKey, Vec<&'a SpanRef>)>;
