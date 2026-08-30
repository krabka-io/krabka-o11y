use super::{AttrValue, OtlpKeyValue, group_attrs, otlp_values};

pub(crate) fn otlp_attrs(attrs: &[(String, AttrValue)]) -> Vec<OtlpKeyValue> {
    group_attrs(attrs)
        .into_iter()
        .map(|(key, values)| OtlpKeyValue {
            key: key.to_string(),
            value: Some(otlp_values(&values)),
            ..OtlpKeyValue::default()
        })
        .collect()
}
