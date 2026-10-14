use super::{
    DistributorError, Labels, OtlpAnyValue, OtlpKeyValue,
    otlp_attribute_labels::attributes_to_labels, otlp_value_to_string,
};

pub(crate) fn otlp_attributes_to_labels(
    attributes: Option<&[OtlpKeyValue]>,
) -> Result<Labels, DistributorError> {
    attributes_to_labels(
        attributes,
        attribute_key,
        attribute_children,
        attribute_label_value,
    )
}

fn attribute_key(attribute: &OtlpKeyValue) -> &str {
    &attribute.key
}

fn attribute_children(attribute: &OtlpKeyValue) -> Option<&[OtlpKeyValue]> {
    match &attribute.value {
        OtlpAnyValue::Kvlist(values) => Some(values.values.as_deref().unwrap_or_default()),
        _ => None,
    }
}

fn attribute_label_value(attribute: &OtlpKeyValue) -> String {
    otlp_value_to_string(&attribute.value)
}
