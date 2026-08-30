use super::ProfilesError;

pub(crate) fn table_ref_checked(
    index: i32,
    len: usize,
    message: &str,
) -> Result<u64, ProfilesError> {
    let idx = usize::try_from(index).map_err(|_| ProfilesError::Invalid(message.to_string()))?;
    if idx >= len {
        return Err(ProfilesError::Invalid(message.to_string()));
    }
    Ok(u64::try_from(idx + 1).unwrap_or(u64::MAX))
}
