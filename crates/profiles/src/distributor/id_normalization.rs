use super::{HashMap, ProfilesError};

pub(crate) fn normalize_required_pprof_id(
    id: u64,
    refs: &HashMap<u64, u32>,
    field: &str,
) -> Result<u32, ProfilesError> {
    refs.get(&id)
        .copied()
        .ok_or_else(|| ProfilesError::Decode(format!("{field} references missing id {id}")))
}

pub(crate) fn normalize_optional_pprof_id(
    id: u64,
    refs: &HashMap<u64, u32>,
    field: &str,
) -> Result<u32, ProfilesError> {
    if id == 0 {
        return Ok(0);
    }
    normalize_required_pprof_id(id, refs, field)
}

pub(crate) fn u32_from_i64(value: i64, field: &str) -> Result<u32, ProfilesError> {
    u32::try_from(value)
        .map_err(|err| ProfilesError::Decode(format!("{field} does not fit u32: {err}")))
}
