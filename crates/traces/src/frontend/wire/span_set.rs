use super::{AttrValue, SpanRef, SpanSet, SpanSetJson};

impl From<&SpanSetJson> for SpanSet {
    fn from(ss: &SpanSetJson) -> Self {
        SpanSet {
            spans: ss.spans.iter().map(SpanRef::from).collect(),
            matched: ss.matched,
            attributes: ss
                .attributes
                .iter()
                .map(|kv| (kv.key.clone(), AttrValue::from(&kv.value)))
                .collect(),
        }
    }
}
