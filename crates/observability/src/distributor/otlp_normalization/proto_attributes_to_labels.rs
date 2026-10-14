use super::{
    DistributorError, Labels, ProtoKeyValue, otlp_attribute_labels::attributes_to_labels,
    proto_any_value, proto_value_to_string,
};

pub(crate) fn proto_attributes_to_labels(
    attributes: Option<&[ProtoKeyValue]>,
) -> Result<Labels, DistributorError> {
    attributes_to_labels(
        attributes,
        attribute_key,
        attribute_children,
        attribute_label_value,
    )
}

fn attribute_key(attribute: &ProtoKeyValue) -> &str {
    &attribute.key
}

fn attribute_children(attribute: &ProtoKeyValue) -> Option<&[ProtoKeyValue]> {
    match attribute
        .value
        .as_ref()
        .and_then(|value| value.value.as_ref())
    {
        Some(proto_any_value::Value::KvlistValue(values)) => Some(&values.values),
        _ => None,
    }
}

fn attribute_label_value(attribute: &ProtoKeyValue) -> String {
    attribute
        .value
        .as_ref()
        .map_or_default(proto_value_to_string)
}
