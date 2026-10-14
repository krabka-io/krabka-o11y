use super::{BTreeMap, DistributorError, Value};

pub(crate) fn parse_structured_metadata(
    metadata: Option<&Value>,
) -> Result<BTreeMap<String, String>, DistributorError> {
    let Some(metadata) = metadata else {
        return Ok(BTreeMap::new());
    };
    let Value::Object(metadata) = metadata else {
        return Err(DistributorError::InvalidStructuredMetadata);
    };

    let mut parsed = BTreeMap::new();
    for entry in metadata {
        let value = entry
            .1
            .as_str()
            .ok_or(DistributorError::InvalidStructuredMetadata)?;
        parsed.insert(entry.0.clone(), value.to_string());
    }
    Ok(parsed)
}
