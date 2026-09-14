use super::{
    DistributorError, Labels, OtlpAnyValue, OtlpKeyValue, normalize_otlp_attribute_name,
    otlp_value_to_string,
};

pub(crate) fn otlp_attributes_to_labels(
    attributes: Option<&[OtlpKeyValue]>,
) -> Result<Labels, DistributorError> {
    let mut labels = Labels::new();
    for attribute in attributes.unwrap_or_default() {
        insert_attribute(&mut labels, "", attribute)?;
    }
    Ok(labels)
}

fn insert_attribute(
    labels: &mut Labels,
    prefix: &str,
    attribute: &OtlpKeyValue,
) -> Result<(), DistributorError> {
    if attribute.key.is_empty() {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    let key = if prefix.is_empty() {
        attribute.key.clone()
    } else {
        format!("{prefix}.{}", attribute.key)
    };
    if let OtlpAnyValue::Kvlist(values) = &attribute.value {
        for child in values.values.as_deref().unwrap_or_default() {
            insert_attribute(labels, &key, child)?;
        }
        return Ok(());
    }
    if labels
        .insert(
            normalize_otlp_attribute_name(&key),
            otlp_value_to_string(&attribute.value),
        )
        .is_some()
    {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    Ok(())
}
