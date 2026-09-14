use super::{
    DistributorError, Labels, ProtoKeyValue, normalize_otlp_attribute_name, proto_any_value,
    proto_value_to_string,
};

pub(crate) fn proto_attributes_to_labels(
    attributes: Option<&[ProtoKeyValue]>,
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
    attribute: &ProtoKeyValue,
) -> Result<(), DistributorError> {
    if attribute.key.is_empty() {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    let key = if prefix.is_empty() {
        attribute.key.clone()
    } else {
        format!("{prefix}.{}", attribute.key)
    };
    if let Some(proto_any_value::Value::KvlistValue(values)) = attribute
        .value
        .as_ref()
        .and_then(|value| value.value.as_ref())
    {
        for child in &values.values {
            insert_attribute(labels, &key, child)?;
        }
        return Ok(());
    }
    let value = attribute
        .value
        .as_ref()
        .map(proto_value_to_string)
        .unwrap_or_default();
    if labels
        .insert(normalize_otlp_attribute_name(&key), value)
        .is_some()
    {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    Ok(())
}
