use super::{ResourceAttrs, SpanRef};

pub(crate) type ResourceSpanGroup<'a> = (ResourceAttrs, Vec<&'a SpanRef>);
