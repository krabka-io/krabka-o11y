use super::*;

pub(crate) fn insert_metadata_if_absent(
    metadata: &mut Labels,
    name: &str,
    value: Option<String>,
) -> Result<(), DistributorError> {
    let Some(value) = value else {
        return Ok(());
    };
    if metadata.insert(name.to_string(), value).is_some() {
        return Err(DistributorError::InvalidOtlpAttribute);
    }
    Ok(())
}
