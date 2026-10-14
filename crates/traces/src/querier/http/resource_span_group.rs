use super::{ResourceAttrs, SpanRef};

pub(crate) struct ResourceSpanGroup<'a> {
    pub(crate) attributes: ResourceAttrs,
    pub(crate) spans: Vec<&'a SpanRef>,
}
