use super::*;

pub(crate) fn validate_attrs(attrs: &[KeyValue], limits: &TenantLimits) -> Result<(), TracesError> {
    for attr in attrs {
        let len = attr.key.len()
            + match &attr.value {
                AttrValue::Str(value) => value.len(),
                AttrValue::Bytes(value) => value.len(),
                AttrValue::Int(_) | AttrValue::Double(_) | AttrValue::Bool(_) => 0,
            };
        if len > limits.max_attr_value.bytes_usize() {
            return Err(TracesError::Limit(format!(
                "attribute `{}` exceeds limit {}",
                attr.key,
                limits.max_attr_value.bytes_usize()
            )));
        }
    }
    Ok(())
}
