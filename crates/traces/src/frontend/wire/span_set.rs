use super::{SpanRef, SpanSet, SpanSetJson, attributes_from_json};

impl From<&SpanSetJson> for SpanSet {
    fn from(ss: &SpanSetJson) -> Self {
        SpanSet {
            spans: ss.spans.iter().map(SpanRef::from).collect(),
            matched: ss.matched,
            attributes: attributes_from_json(&ss.attributes),
        }
    }
}
