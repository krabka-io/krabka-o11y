use super::*;
use crate::frontend::wire::{AnyValueJson, ArrayValueJson, KeyValueJson, SpanJson};

pub(crate) fn search_span_json(span: &SpanRef) -> SpanJson {
    SpanJson {
        span_id: hex::encode(span.span_id),
        start_time_unix_nano: span.start_time_unix_nano.to_string(),
        duration_nanos: span.duration.nanos_i64().to_string(),
        attributes: search_attrs_json(&span.attributes),
    }
}

pub(crate) fn search_attrs_json(attrs: &[(String, AttrValue)]) -> Vec<KeyValueJson> {
    group_attrs(attrs)
        .into_iter()
        .map(|(key, values)| KeyValueJson {
            key: key.to_owned(),
            value: if let [value] = values.as_slice() {
                AnyValueJson::from(*value)
            } else {
                AnyValueJson::ArrayValue(ArrayValueJson {
                    values: values.into_iter().map(AnyValueJson::from).collect(),
                })
            },
        })
        .collect()
}
