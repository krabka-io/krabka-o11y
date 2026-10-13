use super::{Attribute, OtlpKeyValue, group_attrs, otlp_values};

pub(crate) fn otlp_attrs(attrs: &[Attribute]) -> Vec<OtlpKeyValue> {
    group_attrs(attrs)
        .into_iter()
        .map(|group| OtlpKeyValue {
            key: group.key.to_string(),
            value: Some(otlp_values(&group.values)),
            ..OtlpKeyValue::default()
        })
        .collect()
}
