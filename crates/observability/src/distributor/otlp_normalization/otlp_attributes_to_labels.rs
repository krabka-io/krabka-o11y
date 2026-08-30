use super::*;

pub(crate) fn otlp_attributes_to_labels(
    attributes: Option<&[OtlpKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        if attribute.key.is_empty() {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
        let name = normalize_otlp_attribute_name(&attribute.key);
        if labels
            .insert(name, otlp_value_to_string(&attribute.value))
            .is_some()
        {
            return Err(DistributorError::InvalidOtlpAttribute);
        }
    }
    Ok(labels)
}
