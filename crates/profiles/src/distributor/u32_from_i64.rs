use super::ProfilesError;

pub(crate) fn u32_from_i64(value: i64, field: &str) -> Result<u32, ProfilesError> {
    u32::try_from(value)
        .map_err(|err| ProfilesError::Decode(format!("{field} does not fit u32: {err}")))
}
