use super::*;

pub(crate) type ResourceSpanGroup<'a> = (ResourceAttrs, Vec<&'a SpanRef>);
