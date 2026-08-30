use super::{SpanSetJson, SpanSet, SpanRef};

impl From<&SpanSetJson> for SpanSet {
    fn from(ss: &SpanSetJson) -> Self {
        SpanSet {
            spans: ss.spans.iter().map(SpanRef::from).collect(),
            matched: ss.matched,
        }
    }
}
