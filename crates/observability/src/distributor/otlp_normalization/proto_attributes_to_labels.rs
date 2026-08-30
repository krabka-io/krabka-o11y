use super::*;

pub(crate) fn proto_attributes_to_labels(
    attributes: Option<&[ProtoKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        if attribute.key.is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let name = normalize_otlp_attribute_name(&attribute.key);
        let value = attribute
            .value
            .as_ref()
            .map(proto_value_to_string)
            .unwrap_or_default();
        if labels.insert(name, value).is_some() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(labels)
}
