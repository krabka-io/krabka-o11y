use super::*;

pub(crate) fn normalize_required_pprof_id(
    id: u64,
    refs: &HashMap<u64, u32>,
    field: &str,
) -> Result<u32, ProfilesError> {
    refs.get(&id)
        .copied()
        .ok_or_else(|| ProfilesError::Decode(format!("{field} references missing id {id}")))
}
