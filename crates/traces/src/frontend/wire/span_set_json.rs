use super::{Deserialize, Serialize, SpanJson, SpanSet};

/// A spanSet: the spans this trace matched plus the matched count.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpanSetJson {
    #[serde(default)]
    pub spans: Vec<SpanJson>,
    #[serde(default)]
    pub matched: u32,
}

impl From<&SpanSet> for SpanSetJson {
    fn from(ss: &SpanSet) -> Self {
        SpanSetJson {
            spans: ss.spans.iter().map(SpanJson::from).collect(),
            matched: ss.matched,
        }
    }
}
