use super::*;

pub(crate) fn attrs_from_otlp(
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<(String, AttrValue)> {
    attrs
        .iter()
        .filter_map(|attr| {
            attr.value
                .as_ref()
                .and_then(attr_value_from_otlp)
                .map(|value| (attr.key.clone(), value))
        })
        .collect()
}
