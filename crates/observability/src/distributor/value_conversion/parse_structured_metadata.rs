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

    metadata
        .iter()
        .map(|(name, value)| {
            let value = value
                .as_str()
                .ok_or(DistributorError::InvalidStructuredMetadata)?;
            Ok((name.clone(), value.to_string()))
        })
        .collect::<Result<BTreeMap<_, _>, DistributorError>>()
}
