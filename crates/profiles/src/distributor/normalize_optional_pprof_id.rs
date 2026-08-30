use super::{HashMap, ProfilesError, normalize_required_pprof_id};

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
