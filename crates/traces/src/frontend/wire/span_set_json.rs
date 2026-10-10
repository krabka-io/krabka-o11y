use std::collections::BTreeMap;

use super::{
    AnyValueJson, ArrayValueJson, Deserialize, KeyValueJson, Serialize, SpanJson, SpanSet,
};

/// A spanSet: the spans this trace matched plus the matched count.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SpanSetJson {
    #[serde(default)]
    pub spans: Vec<SpanJson>,
    #[serde(default)]
    pub matched: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<KeyValueJson>,
}

impl SpanSetJson {
    /// A spanSet that matched exactly `spans`, with no spanSet attributes.
    #[must_use]
    pub fn of_matched_spans(spans: Vec<SpanJson>) -> Self {
        SpanSetJson {
            matched: u32::try_from(spans.len()).unwrap_or(u32::MAX),
            spans,
            attributes: Vec::new(),
        }
    }
}

impl From<&SpanSet> for SpanSetJson {
    fn from(ss: &SpanSet) -> Self {
        SpanSetJson {
            spans: ss.spans.iter().map(SpanJson::from).collect(),
            matched: ss.matched,
            attributes: {
                let mut grouped = BTreeMap::<String, Vec<AnyValueJson>>::new();
                for (key, value) in &ss.attributes {
                    grouped
                        .entry(key.clone())
                        .or_default()
                        .push(AnyValueJson::from(value));
                }
                grouped
                    .into_iter()
                    .map(|(key, mut values)| KeyValueJson {
                        key,
                        value: if values.len() == 1 {
                            values.remove(0)
                        } else {
                            AnyValueJson::ArrayValue(ArrayValueJson { values })
                        },
                    })
                    .collect()
            },
        }
    }
}
