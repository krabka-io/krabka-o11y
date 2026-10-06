use super::{AttrValue, SpanRef};

/// A matched span set.
#[derive(Clone, Debug, PartialEq)]
pub struct SpanSet {
    pub spans: Vec<SpanRef>,
    pub matched: u32,
    pub attributes: Vec<(String, AttrValue)>,
}
